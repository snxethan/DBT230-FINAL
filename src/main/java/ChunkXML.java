import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class ChunkXML {
    private static long file_size = 2L; // change 10 to the desired file size in GB, ex: 10L = 10GB, 1L = 1GB
    private static final long MAX_FILE_SIZE = file_size * 1024 * 1024 * 1024;
    private static int fileCount = 1; // Counter for the number of files created
    private static long currentFileSize = 0; // Current size of the file being written
    private static BufferedWriter writer; // BufferedWriter for writing to the XML file
    private static final String OUTPUT_DIRECTORY = System.getenv("OUTPUT_DIR") != null ? System.getenv("OUTPUT_DIR") : "output/";



    public static void main(String[] args) {
        String inputFilePath = System.getenv("INPUT_FILE"); // Get input file path from environment variable
        if (inputFilePath == null || inputFilePath.isEmpty()) { // Check if the environment variable is set
            System.err.println("Environment variable INPUT_FILE is not set or is empty.");
            System.exit(1);
        }

        Instant startTotal = Instant.now(); // Start total execution time

        try {
            File inputFile = new File(inputFilePath); // Create a File object for the input file
            if (!inputFile.exists()) {
                System.err.println("Input file does not exist: " + inputFilePath); // Check if the input file exists
                System.exit(1);
            }

            System.out.println("Starting XML parsing and splitting...");

            SAXParserFactory factory = SAXParserFactory.newInstance(); // Create a SAXParserFactory
            factory.setFeature("http://javax.xml.XMLConstants/feature/secure-processing", false);
            SAXParser saxParser = factory.newSAXParser(); // Create a SAXParser

            // Ensure output directory exists
            new File(OUTPUT_DIRECTORY).mkdirs();

            // Start parsing and splitting
            saxParser.parse(inputFile, new DefaultHandler() {
                private boolean inPage = false; // Track if we are inside a <page> element

                @Override
                public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                    // Handle start of an element
                    try {
                        if (qName.equals("page")) { // Check if the element is <page>
                            inPage = true;
                            startNewFileIfNeeded(); // Start a new file if needed
                        }

                        if (inPage) { // If we are inside a <page> element
                            StringBuilder elementStart = new StringBuilder(); // StringBuilder to build the element string
                            elementStart.append("<").append(qName); // Start building the element string

                            for (int i = 0; i < attributes.getLength(); i++) { // Loop through attributes
                                elementStart.append(" ").append(attributes.getQName(i)) // Append attribute name
                                        .append("=\"").append(escapeXML(attributes.getValue(i))).append("\""); // Append attribute value
                            }
                            elementStart.append(">"); // Close the element start tag

                            writeToFile(elementStart.toString()); // Write the element start tag to the file
                        }
                    } catch (IOException e) {
                        throw new SAXException(e);
                    }
                }

                @Override
                public void endElement(String uri, String localName, String qName) throws SAXException { // Handle end of an element
                    try {
                        if (inPage) {
                            writeToFile("</" + qName + ">"); // Write the end tag to the file
                        }
                        if (qName.equals("page")) { // Check if the element is </page>
                            inPage = false;
                        }
                    } catch (IOException e) {
                        throw new SAXException(e);
                    }
                }

                @Override
                public void characters(char[] ch, int start, int length) throws SAXException { // Handle character data
                    if (inPage) {
                        try {
                            writeToFile(escapeXML(new String(ch, start, length))); // Write character data to the file
                        } catch (IOException e) {
                            throw new SAXException(e);
                        }
                    }
                }
            });

            // Close the last writer if open
            if (writer != null) {
                writer.write("</mediawiki>"); // Close previous XML file
                writer.close();

                final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("hh:mm a");
                final String time = " (" + LocalTime.now().format(TIME_FORMATTER) + ")";
                System.out.println("Finished writing file: " + (fileCount - 1) + time);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        Instant endTotal = Instant.now(); // End total execution time
        System.out.println("Total execution time: " + Duration.between(startTotal, endTotal).toMillis() + " ms"); // Print total execution time
    }

    private static void startNewFileIfNeeded() throws IOException {
        if (writer == null || currentFileSize > MAX_FILE_SIZE) { // Check if a new file is needed
            if (writer != null) {
                writer.write("</mediawiki>"); // Close previous XML file
                writer.close();
                System.out.println("Finished writing file: " + (fileCount - 1)); // Add sout for finished file
            }
            String fileName = OUTPUT_DIRECTORY + "wiki_" + fileCount++ + ".xml"; // Create new file name
            writer = new BufferedWriter(new FileWriter(fileName)); // Create new BufferedWriter
            writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<mediawiki>\n"); // Start new XML file
            currentFileSize = 0; // Reset current file size

            final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("hh:mm a");
            final String time = " (" + LocalTime.now().format(TIME_FORMATTER) + ")";
            System.out.println("Started writing new file: " + fileName + time); // Add sout for new file creation
        }
    }

    private static void writeToFile(String data) throws IOException {
        writer.write(data);
        currentFileSize += data.getBytes().length; // Update current file size
    }

    private static String escapeXML(String data) {
        // Escape XML special characters, checks for characters that need to be 'escaped' which means replacing them with their corresponding XML entities
        if (data == null) {
            return "";
        }
        return data.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&apos;");
    }
}
