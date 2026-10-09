package edu.illinois.library.cantaloupe.source;

import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.illinois.library.cantaloupe.config.Configuration;
import edu.illinois.library.cantaloupe.config.Key;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * StreamFactory that downloads an S3 object in parallel using byte ranges into
 * a temporary file, then exposes that file as an InputStream. The behavior is
 * controlled by the following configuration: <br>
 * S3Source.parallel.enabled<br>
 * S3Source.parallel.download.pool_size (default 70)<br>
 * S3Source.parallel.file.min_size_mb (default 8)<br>
 * S3Source.parallel.temp_path<br>
 * S3Source.parallel.target_part_size_mb (default 30)<br>
 * S3Source.parallel.max_parts (default 16)<br>
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
	private final S3Client s3Client;
	private Path tempFile;
	private long contentLength;

	private volatile boolean downloaded = false;
	private final Object lock = new Object();

	public S3ParallelStreamFactory(S3ObjectInfoSupplier infoSupplier) throws IOException {
		this.objectInfo = infoSupplier;
		this.s3Client = S3Source.getClientInstance(infoSupplier.get());
	}

	private long fetchContentLength() throws IOException {
		try {
			S3ObjectInfo s3ObjectInfo = objectInfo.get();
			HeadObjectResponse r = s3Client.headObject(HeadObjectRequest.builder().bucket(s3ObjectInfo.getBucketName())
					.key(objectInfo.get().getKey()).build());
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
		// No parallel for file under minFileSizeMB.
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

	    final int finalNumParts = numParts;
	    final long finalPartSize = partSize;

	    LOGGER.debug("Downloading {} MB in {} parts ({} MB each)",
	            contentLength / (1024.0 * 1024.0),
	            finalNumParts,
	            finalPartSize / (1024.0 * 1024.0));

	    //Pré-allouer le fichier
	    try (RandomAccessFile raf = new RandomAccessFile(tempFile.toFile(), "rw")) {
	        raf.setLength(contentLength);
	    }

	    S3ObjectInfo s3ObjectInfo = objectInfo.get();
	    String bucket = s3ObjectInfo.getBucketName();
	    String key = s3ObjectInfo.getKey();

	    List<CompletableFuture<Void>> futures = new ArrayList<>(finalNumParts);
	    AtomicLong total = new AtomicLong(0);
	    AtomicReference<Throwable> firstError = new AtomicReference<>();

	    // FileChannel partagé : write(ByteBuffer, position) est thread-safe (positional write),
	    //    pas besoin de seek + synchronized, pas de RandomAccessFile par thread
	    try (FileChannel channel = FileChannel.open(tempFile.toFile().toPath(),
	            StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {

	        for (int i = 0; i < finalNumParts; i++) {
	            final long startByte = (long) i * finalPartSize;
	            final long endByte = (i == finalNumParts - 1)
	                    ? contentLength - 1
	                    : Math.min((long) (i + 1) * finalPartSize - 1, contentLength - 1);

	            if (startByte >= contentLength) {
	                break;
	            }

	            futures.add(CompletableFuture.runAsync(() -> {
	                // Si une autre part a déjà échoué, on abandonne
	                if (firstError.get() != null) {
	                    return;
	                }
	                try {
	                    GetObjectRequest req = GetObjectRequest.builder()
	                            .bucket(bucket)
	                            .key(key)
	                            .range("bytes=" + startByte + "-" + endByte)
	                            .build();

	                    // Streaming direct dans le fichier — AUCUN buffer complet en mémoire.
	                    //    ResponseInputStream lit par blocs, on écrit au bon offset via channel.write(buf, pos).
	                    try (ResponseInputStream<GetObjectResponse> in = s3Client.getObject(req)) {
	                        byte[] buffer = new byte[64 * 1024]; // 64 KB
	                        long position = startByte;
	                        int read;
	                        while ((read = in.read(buffer)) != -1) {
	                            ByteBuffer bb = ByteBuffer.wrap(buffer, 0, read);
	                            while (bb.hasRemaining()) {
	                                position += channel.write(bb, position);
	                            }
	                            total.addAndGet(read);
	                        }
	                    }
	                } catch (Exception e) {
	                    firstError.compareAndSet(null, e);
	                    throw new RuntimeException("Part failed [" + startByte + "-" + endByte + "]: "
	                            + e.getMessage(), e);
	                }
	            }, S3DownloadPool.get()));
	        }

	        try {
	            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
	        } catch (Exception e) {
	            // 5. Annuler les parts restantes en cas d'échec
	            futures.forEach(f -> f.cancel(true));
	            // Supprimer le fichier temporaire corrompu
	            try {
	                Files.deleteIfExists(tempFile);
	            } catch (IOException ioEx) {
	                LOGGER.warn("Failed to delete temp file after error: {}", tempFile, ioEx);
	            }
	            throw e;
	        }
	    }

	    ThreadPoolExecutor executor = S3DownloadPool.get();
	    LOGGER.debug("POOL: active={}, pool={}, queue={}",
	            executor.getActiveCount(),
	            executor.getPoolSize(),
	            executor.getQueue().size());

	    LOGGER.debug("Download complete: {} bytes", total.get());
	}

	private void cleanup() {
		try {
			Files.deleteIfExists(tempFile);
		} catch (IOException e) {
			LOGGER.warn("Failed to delete {}", tempFile, e);
		}
	}
	
	/**
	 * Calculates the optimal number of parts based on file size and
	 * configuration.
	 *
	 * <p>Principle: we target a part size ({@code target_part_size_mb}) instead of
	 * using hard-coded tiers. The result is bounded by 2 and
	 * {@code max_parts}.
	 *
	 * <p>Configuration keys:<br>
	 * S3Source.parallel.target_part_size_mb (default 30)<br>
	 * S3Source.parallel.max_parts (default 16)</p>
	 */
	private static int calculateOptimalParts(long fileSizeBytes, int minFileSizeMB) {

	    // No parallelism below the minimum threshold.
	    if (fileSizeBytes < (long) minFileSizeMB * 1024 * 1024) {
	        return 1;
	    }

	    final Configuration conf = Configuration.getInstance();

	    final int targetPartSizeMB = Math.max(1,
	            conf.getInt(Key.S3SOURCE_PARALLEL_TARGET_PART_SIZE_MB, 30));
	    final int minParts = 2;
	    final int maxParts = Math.max(minParts,
	            conf.getInt(Key.S3SOURCE_PARALLEL_MAX_PARTS, 16));

	    final double sizeMB = fileSizeBytes / (1024.0 * 1024.0);

	    int parts = (int) Math.round(sizeMB / (double) targetPartSizeMB);

	    return clamp(parts, minParts, maxParts);
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
