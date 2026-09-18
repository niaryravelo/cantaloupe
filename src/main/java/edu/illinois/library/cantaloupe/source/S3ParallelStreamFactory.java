package edu.illinois.library.cantaloupe.source;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.library.cantaloupe.config.Configuration;
import edu.illinois.library.cantaloupe.config.Key;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * StreamFactory that downloads an S3 object in parallel using byte ranges into
 * a temporary file, then exposes that file as an InputStream. The behavior is
 * controlled by the following configuration: <br>
 * S3Source.parallel.enabled<br>
 * S3Source.parallel.download.pool.size (default 70)<br>
 * S3Source.parallel.file.min.size.mb (default 8MB)
 * S3Source.parallel.temp_path<br>
 * 
 * If file contentLength is lower than S3Source.parallel.file.min.size.mb it will use the existing S3StreamFactory class.
 * 
 * IMPORTANT: This class follows EXACTLY the {@link StreamFactory} contract from
 * Cantaloupe 5.0.7 (no close() on the factory; the temporary file is deleted
 * when the returned InputStream is closed).
 * 
 * @author l.Ravelomanantsoa
 */
public class S3ParallelStreamFactory implements StreamFactory {

	private static final int DEFAULT_FILE_MIN_SIZE_MB = 8;

	private static final Logger LOGGER = LoggerFactory.getLogger(S3ParallelStreamFactory.class);

	private S3ObjectInfoSupplier objectInfo;
	private final S3AsyncClient s3Client;
	private Path tempFile;
	private long contentLength;

	private volatile boolean downloaded = false;
	private final Object lock = new Object();

	public S3ParallelStreamFactory(S3ObjectInfoSupplier infoSupplier) throws IOException {
		this.objectInfo = infoSupplier;
		this.s3Client = S3Source.getAsyncClientInstance(infoSupplier.get());
	}

	private long fetchContentLength() throws IOException {
		try {
			S3ObjectInfo s3ObjectInfo = objectInfo.get();
			HeadObjectResponse r = s3Client.headObject(HeadObjectRequest.builder().bucket(s3ObjectInfo.getBucketName())
					.key(objectInfo.get().getKey()).build()).join();
			return r.contentLength();

		} catch (Exception e) {
			throw new IOException("HEAD failed:" + e.getMessage(), e);
		}
	}

	@Override
	public InputStream newInputStream() throws IOException {
		final Configuration configuration = Configuration.getInstance();

		String tempPathStr = configuration.getString(Key.S3SOURCE_PARALLEL_TEMP_PATH,
				Paths.get(System.getProperty("java.io.tmpdir")).toString());
		Path tempDir = Paths.get(tempPathStr);
		this.contentLength = fetchContentLength();
		//No parallel for file under minFileSizeMB
		int minFileSizeMB = configuration.getInt(Key.S3SOURCE_PARALLEL_MIN_FILE_SIZE_MB, DEFAULT_FILE_MIN_SIZE_MB);
		if (this.contentLength < minFileSizeMB * 1024 * 1024) {
			S3StreamFactory factory = new S3StreamFactory(objectInfo);
			LOGGER.debug("S3ParallelStreamFactory uses S3StreamFactory for file ({} MB)",  contentLength / (1024.0 * 1024.0));
	        return factory.newInputStream();
	    }
		
		this.tempFile = Files.createTempFile(tempDir, "cantaloupe-s3-", ".tmp");
		LOGGER.debug("S3ParallelStreamFactory created: {} ({} MB, dir={})", tempFile, contentLength / (1024.0 * 1024.0),
				tempDir);

		if (!downloaded) {
			synchronized (lock) {
				if (!downloaded) {
					try {
						downloadInParallel(minFileSizeMB);
						downloaded = true;
					} catch (Exception e) {
						cleanup();
						throw new IOException("Parallel S3 download failed: " + e.getMessage(), e);
					}

				}
			}
		}
		return new AutoDeletingInputStream(Files.newInputStream(tempFile), tempFile);
	}

	private void downloadInParallel(int minFileSizeMB) throws Exception {
		int numParts = calculateOptimalParts(contentLength, minFileSizeMB);

		long partSize = contentLength / numParts;

		LOGGER.debug("Downloading {} MB in {} parts ({} MB each)", contentLength / (1024.0 * 1024.0), numParts,
				partSize / (1024.0 * 1024.0));

		try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
			raf.setLength(contentLength);
		}

		List<CompletableFuture<Void>> futures = new ArrayList<>(numParts);
		AtomicLong total = new AtomicLong(0);

		S3ObjectInfo s3ObjectInfo = objectInfo.get();
		String bucket = s3ObjectInfo.getBucketName();
		String key = s3ObjectInfo.getKey();

		for (int i = 0; i < numParts; i++) {
			final long startByte = (long) i * partSize;
			final long endByte = (i == numParts - 1) ? contentLength - 1 : (long) (i + 1) * partSize - 1;

			futures.add(CompletableFuture.runAsync(() -> {
				try {
					GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key)
							.range("bytes=" + startByte + "-" + endByte).build();

					ResponseBytes<GetObjectResponse> bytes = s3Client.getObject(req, AsyncResponseTransformer.toBytes())
							.join();

					try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
						raf.seek(startByte);
						raf.write(bytes.asByteArray());
					}
					total.addAndGet(bytes.asByteArray().length);
				} catch (Exception e) {
					throw new RuntimeException("Part failed: " + e.getMessage(), e);
				}
			}, S3DownloadPool.get()));
		}

		CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
		ThreadPoolExecutor executor = S3DownloadPool.get();
		int active = executor.getActiveCount();
		int exPoolSize = executor.getPoolSize();
		int queueSize = executor.getQueue().size();
		LOGGER.debug("POOL: active={}, pool={}, queue={}", active, exPoolSize,
				queueSize);

	}

	private void cleanup() {
		try {
			Files.deleteIfExists(tempFile);
		} catch (IOException e) {
			LOGGER.warn("Failed to delete {}", tempFile, e);
		}
	}

	public static int calculateOptimalParts(long fileSizeBytes, int minFileSizeMB) {
		
		//No parallel for file under minFileSizeMB 
		if (fileSizeBytes < minFileSizeMB * 1024 * 1024) { 
	        return 1; 
	    }
		
		final double sizeMB = fileSizeBytes / (1024.0 * 1024.0);

		if (sizeMB < 50) {
			return sizeMB < 25 ? 2 : 3;
		}

		if (sizeMB < 200) {
			// ~21 MB par part
			return clamp((int) Math.round(sizeMB / 21.0), 5, 8);
		}

		if (sizeMB < 500) {
			// ~30 MB par part
			return clamp((int) Math.round(sizeMB / 30.0), 8, 12);
		}

		if (sizeMB < 1024) {
			// ~35 MB par part
			return clamp((int) Math.round(sizeMB / 35.0), 12, 16);
		}

		// > 1 GB : ~55 MB par part
		return Math.max(16, (int) Math.round(sizeMB / 55.0));
	}

	private static int clamp(int value, int min, int max) {
		return Math.max(min, Math.min(value, max));
	}

	/**
	 * An input stream that will delete a file on close.
	 */
	private static class AutoDeletingInputStream extends FilterInputStream {

		private final Path fileToDelete;

		public AutoDeletingInputStream(InputStream in, Path fileToDelete) throws FileNotFoundException {
			super(in);
			this.fileToDelete = fileToDelete;
		}

		@Override
		public void close() throws IOException {
			try {
				super.close();
			} finally {
				try {
					Files.deleteIfExists(fileToDelete);
				} catch (IOException e) {
					// Log only
				}
			}
		}

	}
	
	/**
	 * Shared, lazily-created {@link ThreadPoolExecutor} for S3 parallel stream I/O operations.
	 *
	 * <p>Singleton (per classloader). The pool is fixed-size, uses daemon threads
	 * named {@code s3-io-N}, and is shut down via a JVM shutdown hook. Callers
	 * should not shut it down.</p>
	 */
	private final class S3DownloadPool {

		private S3DownloadPool() {
		}

		/**
		 * Initialization-on-demand holder: loaded and initialized only on the first
		 * call pto {@link #get()}. The JVM guarantees this is thread-safe and happens
		 * exactly once, so no explicit locking is needed.
		 */
		public static ThreadPoolExecutor get() {
			return Holder.INSTANCE;
		}

		private static final class Holder {

			private static final int DEFAULT_POOL_SIZE = 70;
			private static final ThreadPoolExecutor INSTANCE = create();

			/** Builds the pool and registers a JVM shutdown hook. */
			private static ThreadPoolExecutor create() {
				final Configuration conf = Configuration.getInstance();
				int poolSize = conf.getInt(Key.S3SOURCE_PARALLEL_DOWNLOAD_POOL_SIZE, DEFAULT_POOL_SIZE);
				long keepAliveTimeInSeconds = 60L;// not used as coreSize == maxSize
				ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, keepAliveTimeInSeconds,
						TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
							Thread t = new Thread(r, "s3-io");
							t.setDaemon(true);
							return t;
						});
				int active = executor.getActiveCount();
				int exPoolSize = executor.getPoolSize();
				int queueSize = executor.getQueue().size();

				LOGGER.debug("POOL: active={}, pool={}, queue={}", active, exPoolSize,
						queueSize);

				Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdownNow, "s3-io-shutdown"));
				return executor;
			}

		}

	}
}
