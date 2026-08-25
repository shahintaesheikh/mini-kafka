/*Fundamental unit of data storage in Kafka, segments of message logs*/

package com.simplekafka.broker;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Partition{
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());
    private static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024;
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessLog activeLogfile; 
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;

    public Partition(int id, int leader, List<Integer> followers, String baseDir){
        this.id = id;
        this.leader = leader;
        this.followers = followers;
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();

        initialize();
    }

    private void initialize(){
        try{
            
            //create directory if it doesn't exist
            File dir = new File(baseDir);
            if(!dir.exists()){
                dir.mkdirs();
            }

            //list existing log files
            File[] files = dir.listFiles((dir1, name) -> name.endsWith(LOG_SUFFIX));
            if(files != null && files.length > 0){
                for(File file: files){
                    //strip the log suffix
                    String baseName = file.getName().substring(0, file.getName().length()-LOG_SUFFIX.length());
                    Long baseOffset = Long.parseOffset(baseName);

                    //create new index file 
                    File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
                    if(indexFile.exists()){
                        //create a segment if the index file exists for the new file
                        SegmentInfo segment = new SegmentInfo(baseOffset, file.getAbsolutePath(), indexFile.getAbsolutePath());
                        segments.add(segment);
                    }
                }

                //sort segments by base offset (when they were created)
                segments.sort((s1, s2) -> Long.compare(s1.getBaseOffset(), s2.getBaseOffset()))

                //determine the next segment from the last segments offset
                if(!segments.isEmpty()){
                    SegmentInfo lastSegment = segments.get(segments.size()-1);
                    nextOffset.set(lastSegment.getBaseOffset() + countMessagesInSegment(lastSegment)); 
                }
            }

            //create segments if there are none
            if (segments.isEmpty()){
                createNewSegment(0);
            }else{
                //add append segments otherwise
                SegmentInfo lastSegment = segments.get(segments.size()-1);
                openSegmentForAppend(lastSegment);
            }

            LOGGER.info("Initialized partition" + id + " with " + segments.size() + " segments, next offset: " + nextOffset.get());

        } catch (Exception e){
            LOGGER.log(Level.SEVERE, "Failed to initialize partition " + id, e);
        }
    }

    private long countMessagesInSegment(SegmentInfo segment) throws IOException {
        long count = 0;

        //try to create RandomAccessFile for file seek capability, logChannel object for position tracking and actual byte reading of the .log file
        try (RandomAccessFile logFile = new RandomAccessFile(segment.getLogPath(), "r");
        FileChannel logChannel = logFile.getChannel()){
            ByteBuffer buffer = ByteBuffer.allocate(4);

            //iterate through messages
            while (logChannel.position() < logChannel.size()){
                buffer.clear();
                int bytesRead = logChannel.read(buffer);
                if (bytesRead < 4) break;

                //flip buffer for read capability
                buffer.flip();
                int messageSize = buffer.getInt();

                //skip the actual message bytes and increment count
                logChannel.position(logChannel.position() + messageSize);
                count++;
            }
        }

        return count; 
    }

    /*create new segment in partition*/
    private void createNewSegment(long baseOffset) throws IOException {
        String baseName = String.format("%020d", baseOffset);
        String logPath = baseDir + File.separator + baseName + LOG_SUFFIX
        String indexPath = baseDir + File.separator + baseName + INDEX_SUFFIX

        File logFile = new File(logPath);
        logFile.createNewFile();

        File indexFile = new File(indexPath);
        indexFile.createNewFile();

        //create new segment with defined log and index file
        SegmentInfo segment = new SegmentInfo(baseOffset, logPath, indexPath);
        segments.add(segment)


        openSegmentForAppend(segment);

        LOGGER.info("Created new segment for partition " + id + ", base offset: " + baseOffset);
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        //close current log channel if there is one open currently
        if (activeLogChannel != null && activeLogChannel.isOpen()){
            activeLogChannel.close();
        }

        if (activeLogFile != null){
            activeLogFile.close();
        }

        //open the segment
        activeLogFile = new RandomAccessFile(segment.getLogPath(), "rw");
        activeLogChannel = activeLogFile.getChannel();

        //move position of channel to the end for appending
        activeLogChannel.position(activeLogChannel.size());
    }

    //append message to a .log file
    public long append(byte[] message){
        //lock log so that you this is the only producer writing to the log
        lock.writeLock().lock();
        try{
            //get the current offset from the next offset instance variable set at the partition
            long currentOffset = nextOffset.get();
            
            //check if we need to roll to next segment
            if (activeLogChannel.position() >= DEFAULT_SEGMENT_SIZE){
                activeLogChannel.close();
                activeLogFile.close();
                createNewSegment(currentOffset);
            }

            //write message size and data
            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length());
            buffer.put(message);
            buffer.flip();

            //write to file
            long position = activeLogChannel.position();
            activeLogChannel.write(buffer);
            //force write to disk
            activeLogChannel.force(true);

            //update index
            updateIndex(currentOffset, position)

            nextOffset.incrementAndGet();

            return currentOffset;
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, "Failed to append message to partition " + id, e);
            return -1;
        } finally{
            lock.writeLock().unlock();
        }
    }
}