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

public class Application {

    public static void main(String... args) throws InterruptedException, IOException {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(false);

        try (Ignite ignite = Ignition.start(cfg)) {
            CacheConfiguration<Integer, LabeledImage> cacheCfg = new CacheConfiguration<>();
            cacheCfg.setName("MNIST_CACHE");
            cacheCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

            IgniteCache<Integer, LabeledImage> cache = ignite.createCache(cacheCfg);

            System.out.println("Filling cache...");
            fillCache(cache);
            System.out.println("Cache is ready");

            Thread.currentThread().join();
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
            double[] pixels = new double[imagesRows * imagesCols];
            for (int j = 0; j < imagesRows * imagesCols; j++) {
                byte px = images.readByte();
                pixels[j] = (px + 128) / 255;
            }
            int lb = labels.readByte();
            cache.put(i, new LabeledImage(pixels, lb));
        }
    }

    private static class LabeledImage {

        private final double[] pixels;

        private final int label;

        public LabeledImage(double[] pixels, int label) {
            this.pixels = pixels;
            this.label = label;
        }

        public double[] getPixels() {
            return pixels;
        }

        public int getLabel() {
            return label;
        }
    }
}
