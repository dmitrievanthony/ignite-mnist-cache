package test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import picocli.CommandLine;

public class Application {

    public static void main(String... args) {
        CommandLine.run(new Command(), System.out, args);
    }

    @CommandLine.Command()
    private static class Command implements Runnable {

        @CommandLine.Option(names = {"-c", "--config"}, description = "Apache Ignite configuration")
        private String cfg;

        @Override public void run() {
            try (Ignite ignite = getIgnite()) {
                if (ignite.cacheNames().contains("MNIST_CACHE"))
                    System.out.println("Apache Ignite cluster already contains MNIST_CACHE");
                else {
                    CacheConfiguration<Integer, LabeledImage> cacheCfg = new CacheConfiguration<>();
                    cacheCfg.setName("MNIST_CACHE");
                    cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 8));

                    IgniteCache<Integer, LabeledImage> cache = ignite.getOrCreateCache(cacheCfg);
                    fillCache(cache);

                    System.out.println("MNIST_CACHE created");
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private Ignite getIgnite() {
            if (cfg != null)
                return Ignition.start(cfg);
            else {
                IgniteConfiguration cfg = new IgniteConfiguration();
                cfg.setClientMode(true);

                return Ignition.start(cfg);
            }
        }
    }

    private static void fillCache(IgniteCache<Integer, LabeledImage> cache) throws IOException {
        DataInputStream images = new DataInputStream(Application.class.getClassLoader()
            .getResourceAsStream("test/train-images-idx3-ubyte"));
        DataInputStream labels = new DataInputStream(Application.class.getClassLoader()
            .getResourceAsStream("test/train-labels-idx1-ubyte"));

        images.readInt();
        int imagesLen = images.readInt();
        int imagesRows = images.readInt();
        int imagesCols = images.readInt();

        labels.readInt();
        int labelsLen = labels.readInt();

        if (imagesLen != labelsLen)
            throw new IllegalStateException("Number of images and labels are not equal");

        for (int i = 0; i < imagesLen; i++) {
            byte[] pixels = new byte[imagesRows * imagesCols];
            for (int j = 0; j < imagesRows * imagesCols; j++) {
                byte px = images.readByte();
                pixels[j] = px;
            }
            byte lb = labels.readByte();
            cache.put(i, new LabeledImage(pixels, lb));
        }
    }

    private static class LabeledImage {

        private final byte[] pixels;

        private final int label;

        public LabeledImage(byte[] pixels, byte label) {
            this.pixels = pixels;
            this.label = label;
        }

        public byte[] getPixels() {
            return pixels;
        }

        public int getLabel() {
            return label;
        }
    }

}
