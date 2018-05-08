import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.microsoft.rest.v2.http.HttpClient;
import com.microsoft.rest.v2.http.HttpClientConfiguration;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.rest.v2.util.FlowableUtil;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;


public class Main {
    public static void main(String[] args) throws InvalidKeyException, IOException, URISyntaxException, StorageException, InterruptedException {
        //Thread.sleep(5000);
        //uploadV7();
        uploadV10();
    }

    private static String getAccountName() {
        return "xclientperf06";
        //return System.getenv("ACCOUNT_NAME");
    }

    private static String getAccountKey() {
        return "F3MCthcnltt54tSIF8S2egtoW2Jya9F5Zm05nhzlJR0sgiaLARIyI0+LOeJK/Q4Ljkr792Knh75AVgRK0/0vSA==";
        //return System.getenv("ACCOUNT_KEY");
    }

    private static void uploadV7() throws URISyntaxException, InvalidKeyException, StorageException, InterruptedException {
        System.out.println("Storage SDK V7");
        CloudStorageAccount account = CloudStorageAccount.parse("DefaultEndpointsProtocol=https;AccountName=xclientperf06;AccountKey=F3MCthcnltt54tSIF8S2egtoW2Jya9F5Zm05nhzlJR0sgiaLARIyI0+LOeJK/Q4Ljkr792Knh75AVgRK0/0vSA==;EndpointSuffix=core.windows.net");

        CloudBlobClient blobClient = account.createCloudBlobClient();

        CloudBlobContainer container = blobClient.getContainerReference("javacomparison1" + System.currentTimeMillis());
        container.create();

        ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10000);
        for (int i=0; i < 10; i++) {
            queue.add(i);
        }

        ArrayList<Thread> threads = new ArrayList<>();
        long startTime = System.nanoTime();
        for (int i=0; i<5; i++) {
            Thread t = new Thread(() -> {
                while(!queue.isEmpty()) {
                    try {
                        Integer I = queue.take();
                        CloudBlockBlob blob = container.getBlockBlobReference("syncBlob" + I);
                        blob.uploadFromFile("myfile.txt");
                        System.out.println("V7 Upload " + I + ": " + System.nanoTime());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println("Total time to upload " + totalTime);
        container.deleteIfExists();
        System.out.println("End V7 run");
    }

    private static void uploadV10() {
        String accountName = getAccountName();
        String accountKey = getAccountKey();
        ContainerURL containerURL;

        try {


            System.out.println("New Storage SDK");

            PipelineOptions po = new PipelineOptions();
            HttpClientConfiguration configuration = new HttpClientConfiguration(
                    new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)));
            po.client = HttpClient.createDefault();//configuration);

            // Create a ServiceURL to call the Blob service. We will also use this to construct the ContainerURL
            SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);

            final ServiceURL serviceURL = new ServiceURL(new URL("http://" + accountName + ".blob.core.windows.net"), StorageURL.createPipeline(creds, po));

            // Let's create a container using a blocking call to Azure Storage
            containerURL = serviceURL.createContainerURL("javacomparison2"+ System.currentTimeMillis());
            containerURL.create(null, null).blockingGet();

            long startTime = System.nanoTime();

            Observable.range(0, 10)
                    .flatMap(i -> {
                        BlockBlobURL asyncblob = containerURL.createBlockBlobURL("asyncblob" + i);
                        TransferManager.UploadToBlockBlobOptions asyncOptions = new TransferManager.UploadToBlockBlobOptions(
                                null, null, null, null, 1);

                        return TransferManager.uploadFileToBlockBlob(
                                FileChannel.open(Paths.get("myfile.txt")), asyncblob,
                                BlockBlobURL.MAX_PUT_BLOCK_BYTES, asyncOptions).toObservable()
                                .doAfterTerminate(() -> System.out.println("V10 Upload " + i + ": " + System.nanoTime()));
                    }, 2)
                    .blockingSubscribe();

            long endTime   = System.nanoTime();
            long totalTime = endTime - startTime;
            System.out.println("Total time to upload " + totalTime);
            containerURL.delete(null).blockingGet();

        } catch (InvalidKeyException e) {
            System.out.println("Invalid Storage account name/key provided");
        } catch (MalformedURLException e) {
            System.out.println("Invalid URI provided");
        }
    }
}
