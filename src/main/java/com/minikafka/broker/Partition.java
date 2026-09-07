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

    /*Update an index file with a new offset*/
    private void updateIndex(long offset, long position){
        try{
            //find the current segment
            if (segments.isEmpty()) return;

            SegmentInfo currSegment = segments.get(segments.size()-1);

            try(RandomAccessFile indexFile = new RandomAccessFile(currSegment.getIndexPath(), "rw");
                FileChannel indexChannel = indexFile.getChannel()){
                    //start at end of index file
                    indexChannel.position(indexChannel.size());

                    //write offset
                    ByteBuffer buffer = ByteBuffer.allocate(16);

                    buffer.putLong(offset);
                    buffer.putLong(position);
                    buffer.flip();

                    indexChannel.write(buffer);
                    indexChannel.force(true);
                }
        } catch (IOException e){
            LOGGER.log(Level.SEVERE, "Failed to update index for partition " + id, e);
        }
    }

    /*Read messages from log starting at offset */
    public List<byte[]> readMessages(long offset, int maxBytes){
        //set up read lock
        lock.readLock().lock()
        List<byte[]> messages = new ArrayList<>();
        int bytesRead = 0;

        try{
            //find the segment that has the offset 
            SegmentInfo targetSegment = findSegmentForOffset(offset);
            if (targetSegment == null){
                return messages;
            }

            //find file position for offset using indices
            long position = findPositionForOffset(targetSegment, offset);
            if (position < 0){
                return messages;
            }

            //use channels for reading, same pattern as before
            try(RandomAccessFile logFile = new RandomAccessFile(targetSegment.getLogPath(),"rw");
                FileChannel logChannel = logFile.getChannel()){
                    //same pattern as in other functions
                    fileChannel.position(position);

                    //read messages until the max bytes is reached
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    long currOffset = offset;

                    while (bytesRead < maxBytes && logChannel.position() < logChannel.size()){
                        //read the message size
                        sizeBuffer.clear();
                        int sizeRead = logChannel.read(sizeBuffer);
                        if (sizeRead < 4) break;
                        //switch to read mode
                        sizeBuffer.flip();
                        int messageSize = sizeBuffer.getInt();

                        if (bytesRead + messageSize > maxBytes) break;

                        //write into the message buffer
                        ByteBuffer messageBuffer = ByteBuffer.allocate(messageSize);
                        int messageRead = logChannel.read(messageBuffer);

                        if (messageRead < messageSize) {
                            LOGGER.warning("Incomplete message read at offset " + currOffset);
                            break;
                        }

                        messageBuffer.flip();

                        //add message result to messages
                        byte[] message = new byte[messageSize];
                        messageBuffer.get(message);
                        messages.add(message);

                        //update bytesRead
                        bytesRead += 4;
                        currentOffset++;

                        //check if we're at the end of current segment
                        if(logChannel.position() >= logChannel.size() && currentOffset < nextOffset.get()){
                            //switch index to next segment
                            int nextSegmentIndex = segments.indexOf(targetSegment) + 1;
                            if (nextSegmentIndex < segments.size()){
                                logChannel.close();
                                logFile.close();

                                targetSegment = segments.get(nextSegmentIndex);

                                RandomAccessFile nextLogFile = new RandomAccessFile(targetSegment.getLogPath(), "r");
                                FileChannel nextLogChannel = nextLogFile.getChannel();

                                //continue reading
                                position = 0;
                                nextLogChannel.position(position);
                            }
                        }
                    }



                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to read messages from partition " + id, e);
                } finally {
                    lock.readLock().unlock();
                }
        }

        return messages;
    }

    /*find segment giving given offset*/
    private SegentInfo findSegmentForOffset(long offset){
        if (segments.isEmpty() || offset >= nextOffset.get()){
            return null;
        }

        //binary search to find segment
        int low = 0;
        int high = segments.size() - 1;

        while (low <= high){
            int mid = (low + high)/2;
            SegmentInfo segment = segments.get(mid);

            if(mid < segments.size()-1){
                SegmentInfo nextSegment = segments.get(mid+1);
                if (offset >= segment.getBaseOffset() && offset < nextSegment.getBaseOffset()){
                    return offset;
                }
            //if not within then check if it's the last segment
            }else{
                if (offset >= segment.getBaseOffset()){
                    return segment;
                }
            }

            //complete binary search
            if (offset < segment.getBaseOffset()) {
                high = mid -1
            }else{
                low = mid + 1;
            }
        }

        return null;
    }

    /*find file position for given offset*/
    private long findPositionForOffset(SegmentInfo segment, long offset){
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.getIndexPath, "r");
            FileChannel indexChannel = indexFile.getChannel()){
                
                if(indexChannel.size() == 0){
                    //empty index
                    return 0;
                }

                //relative offset within segment
                long relativeOffset = offset - segment.getBaseOffset();

                //each index entry is 16 bytes (8 for offset, 8 for position)
                long entryCount = indexChannel.size()/16;

                if (relativeOffset >= entryCount){
                    //not found in index, use last position
                    indexChannel.position(indexChannel.size()-16);
                    ByteBuffer buffer = ByteBuffer.allocate(16);
                    indexChannel.read(16);
                    buffer.flip();

                    buffer.getLong();
                    return buffer.getLong();
                }

                //read index entry
                indexChannel.position(relativeOffset*16);
                ByteBuffer buffer = ByteBuffer.allocate(16);
                indexChannel.read(buffer);
                buffer.flip();

                buffer.getLong();
                return buffer.getLong();
            } catch (IOException e){
                LOGGER.log(Level.SEVERE, "Failed to find position for offset " + offset, e);
                return -1;
            }
    }

    public int getId(){
        return id;
    }

    public int getLeader(){
        return leader;
    }

    public void setLeader(int leader){
        this.leader = leader;
    }

    public List<Integer> getFollowers(){
        return new ArrayList<>(followers);
    }

    public long getLogEndOffset(){
        return nextOffset.get();
    }

}