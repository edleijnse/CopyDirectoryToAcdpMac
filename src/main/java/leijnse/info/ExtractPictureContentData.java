package leijnse.info;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractPictureContentData {
    private Map<String, Integer> locTags = new TreeMap<>();

    private String extractTags(String iTags) {
        String oTags = iTags;
        String[] arrayOfStr = iTags.split("%");
        for (String myTagString : arrayOfStr) {
            Integer myValue = locTags.get(myTagString);
            if (myValue != null) {
                myValue++;
            } else {
                myValue = 1;
            }

            locTags.put(myTagString, myValue);
        }

        return oTags;
    }

    public Map<String, Integer> topTags(Map<String, Integer> inputTags, Integer topCount) {

        // https://dzone.com/articles/how-to-sort-a-map-by-value-in-java-8
        final Map<String, Integer> sortedByCount = inputTags.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        Map<String, Integer> topTags  = sortedByCount.entrySet().stream().limit(topCount)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        return topTags;
    }

    public Map<String, Integer> extractVisionTags(String iFileName) throws IOException {
        try {
            String fileName = iFileName;
            Stream<String> lines = Files
                    .lines(Paths.get(fileName))
                    .filter(line -> line.contains("%"))
                    .map(line -> {
                        int myIndexContentBegin = StringUtils.ordinalIndexOf(line, ",", 73);
                        int myIndexContentEnd = StringUtils.ordinalIndexOf(line, ",", 74);
                        int myIndexTagsBegin = StringUtils.ordinalIndexOf(line, ",", 74);
                        int myIndexImageEnd = StringUtils.ordinalIndexOf(line, ",", 1);
                        String imageName = line.substring(0, myIndexImageEnd);
                        String imageDescription = line.substring(myIndexContentBegin + 1, myIndexContentEnd);
                        String imageTags = line.substring(myIndexTagsBegin + 1);
                        String oTags = extractTags(imageTags);
                        String myOutput = imageName + ";" +
                                imageDescription + ";" +
                                oTags;
                        return myOutput;
                    });
            // lines.forEach(System.out::println);
            // any action required on lines, for instance count:
            lines.count();
            lines.close();

        } catch (IOException io) {
            io.printStackTrace();
        }

        return locTags;
    }

    private static String subscriptionKey = "MY-KEY-HERE";


    public static final String uriBase =
            "https://westeurope.api.cognitive.microsoft.com/vision/v1.0/analyze";
    //    "https://northeurope.api.cognitive.microsoft.com/vision/v1.0/analyze";



    public void setSubstringKey(String myKey) {
        subscriptionKey = myKey;
    }

    public File compressJpg(File imageFile) throws IOException {
        System.out.println(imageFile.getAbsolutePath());
        String myNewFileName = imageFile.getAbsolutePath();
        myNewFileName = myNewFileName.replace(".jpg", "_compressed.jpg");
        System.out.println(myNewFileName);
        // File compressedImageFile = new File (myNewFileName);
        File compressedImageFile = File.createTempFile("pattern", ".suffix");

        try (InputStream is = new FileInputStream(imageFile)) {
            OutputStream os = new FileOutputStream(compressedImageFile);
            float quality = 0.30f;
            // create a BufferedImage as the result of decoding the supplied InputStream
            BufferedImage image = ImageIO.read(is);
            // get all image writers for JPG format
            Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpg");
            if (!writers.hasNext()) {
                throw new IllegalStateException("No writers found");
            }
            ImageWriter writer = (ImageWriter) writers.next();
            ImageOutputStream ios = ImageIO.createImageOutputStream(os);
            writer.setOutput(ios);
            ImageWriteParam param = writer.getDefaultWriteParam();
            // compress to a given quality
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(quality);
            // appends a complete image stream containing a single image and
            //associated stream and image metadata and thumbnails to the output
            writer.write(null, new IIOImage(image, null, null), param);

            // close all streams
            compressedImageFile.deleteOnExit();
            is.close();
            os.close();
            ios.close();
            writer.dispose();


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return compressedImageFile;
    }


    public static PictureMetaData getPictureContent(File file) throws Exception {


        PictureMetaData myPictureMetadata = ExtractPictureMetaData.getPictureMetaDataExif(file);


        System.out.println("getPictureContent start");
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            System.out.println("getPictureContent httpClient start");
            // use httpClient (no need to close it explicitly)
            URIBuilder builder = new URIBuilder(uriBase);

            builder.setParameter("visualFeatures", "Categories,Description,Color,Adult");
            builder.setParameter("language", "en");

            URI uri = builder.build();
            HttpPost request = new HttpPost(uri);

            // https://stackoverflow.com/questions/49463736/how-to-send-a-local-image-instead-of-url-to-microsoft-computer-vision-api-using
            //  request.setHeader("Content-Type", "application/json");
            request.setHeader("Content-Type", "application/octet-stream");
            request.setHeader("Ocp-Apim-Subscription-Key", subscriptionKey);

            // Request body.
            // BufferedImage image = null;
            // File f = null;
            // f = new File("C:\\Coffee.jpg"); //image file path
            // image = ImageIO.read(file);

            // File file = new File("C:\\Coffee.jpg");

            FileEntity reqEntityF =
                    new FileEntity(file, ContentType.APPLICATION_OCTET_STREAM);

            request.setEntity(reqEntityF);

            HttpResponse response = httpClient.execute(request);
            HttpEntity entity = response.getEntity();

            if (entity != null) {
                // Format and display the JSON response.
                String jsonString = EntityUtils.toString(entity);
                JSONObject json = new JSONObject(jsonString);
                // System.out.println("REST Response:\n");
                // System.out.println(json.toString(2));
                String myCode = "";
                try {
                    myCode = json.getString("code");
                    // System.out.println("Code: " + myCode);
                } catch (Exception e) {

                }

                if (myCode.contains("InvalidImageSize")) {
                    System.out.println("Invalid image size");
                    return myPictureMetadata;
                }
                JSONArray descriptionArr = json.getJSONObject("description").getJSONArray("captions");
                descriptionArr.forEach(element -> {
                    JSONObject myElement = (JSONObject) element;
                    String textContent = myElement.getString("text");
                    // System.out.println("content: " + textContent);
                    myPictureMetadata.setVISION_CONTENT(Optional.ofNullable(textContent));
                });
                JSONArray tagsArr = json.getJSONObject("description").getJSONArray("tags");
                final String[] myTagsTmp = {""};
                tagsArr.forEach(element -> {
                    String tagName = element.toString();
                    // System.out.println(tagName);
                    myTagsTmp[0] += element.toString() + "%";

                });
                String myTags = myTagsTmp[0];
                myPictureMetadata.setVISION_TAGS(Optional.ofNullable(myTags));

            }

        } catch (IOException e) {

            // handle
            e.printStackTrace();

        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            System.out.println("JSON Exception: " + e.getLocalizedMessage());

        }


        return myPictureMetadata;
    }
}
