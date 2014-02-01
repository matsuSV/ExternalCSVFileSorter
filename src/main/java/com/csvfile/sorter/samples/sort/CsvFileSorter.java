package com.csvfile.sorter.samples.sort;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.csvfile.sorter.samples.serialize.RowData;

/*
 * 数GBになるCSVファイルをソートするため、公開されているソート処理を本プロジェクトに取り込んだ。
 * コメントなどもそのまま抜粋しているため、暫時修正を行っていく。
 */

/**
 * http://code.google.com/p/externalsortinginjava/
 *
 * Goal: offer a generic external-memory sorting program in Java.
 *
 * It must be : - hackable (easy to adapt) - scalable to large files - sensibly
 * efficient.
 *
 * This software is in the public domain.
 *
 * Usage: java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 *
 * You can change the default maximal number of temporary files with the -t
 * flag: java com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 * -t 3
 *
 * For very large files, you might want to use an appropriate flag to allocate
 * more memory to the Java VM: java -Xms2G
 * com/google/code/externalsorting/ExternalSort somefile.txt out.txt
 *
 * By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 * Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Jerry Yang First
 * published: April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-sorting-in-java/
 */
public final class CsvFileSorter {
   
    private static final int FLUSH_INTERVAL = 10000;

    /**
     * コンストラクタ.
     */
    private CsvFileSorter() {
    }

    private static final int OBJ_HEADER;
    private static final int ARR_HEADER;
    private static final int INT_FIELDS = 12;
    private static final int OBJ_REF;
    private static final int OBJ_OVERHEAD;
    private static final int DEFAULTMAXTEMPFILES = 1024;
    private static boolean is64bitJVM;

    public static final int BUFFER_SIZE = 4096;

    /**
     * Class initializations.
     */
    static {
        // By default we assume 64 bit JVM
        // (defensive approach since we will get
        // larger estimations in case we are not sure)
        is64bitJVM = true;
        // check the system property "sun.arch.data.model"
        // not very safe, as it might not work for all JVM implementations
        // nevertheless the worst thing that might happen is that the JVM is
        // 32bit
        // but we assume its 64bit, so we will be counting a few extra bytes per
        // string object
        // no harm done here since this is just an approximation.
        String arch = System.getProperty("sun.arch.data.model");
        if (arch != null) {
            if (arch.indexOf("32") != -1) {
                // If exists and is 32 bit then we assume a 32bit JVM
                is64bitJVM = false;
            }
        }
        // The sizes below are a bit rough as we don't take into account
        // advanced JVM options such as compressed oops
        // however if our calculation is not accurate it'll be a bit over
        // so there is no danger of an out of memory error because of this.
        OBJ_HEADER = is64bitJVM ? 16 : 8;
        ARR_HEADER = is64bitJVM ? 24 : 12;
        OBJ_REF = is64bitJVM ? 8 : 4;
        OBJ_OVERHEAD = OBJ_HEADER + INT_FIELDS + OBJ_REF + ARR_HEADER;

    }

    /**
     * Estimates the size of a {@link String} object in bytes.
     *
     * @param line
     *            The string to estimate memory footprint.
     * @return The <strong>estimated</strong> size in bytes.
     */
    public static long estimatedSizeOf(List<String> line) {
        long result = 0L;
        for (String value : line) {
            if (value != null) {
                result += value.length() * 4; // UTF-8の文字長は最大で４バイト必要になるため、valueの大きさを4倍している
            }
        }
        return result + OBJ_OVERHEAD;
    }

    public static void sort(String inputfile, String outputfile) throws IOException {

        // 入力ファイルが存在しないか、存在していてもファイルサイズが0バイトの場合は処理を終了
        File file = new File(inputfile);
        if (!file.isFile() || (file.length() == 0)) {
            return;
        }

        int maxtmpfiles = DEFAULTMAXTEMPFILES;
        Charset cs = Charset.forName("UTF-8");

        Comparator<List<String>> comparator = new Comparator<List<String>>() {
            @Override
            public int compare(List<String> r1, List<String> r2) {
                int result = 0;
                int size = r1.size() < r2.size() ? r1.size() : r2.size();
                for (int i = 0; i < size && result == 0; i++) {
                    String str1 = r1.get(i);
                    String str2 = r2.get(i);
                    if (str1 == null) {
                        str1 = "";
                    }
                    if (str2 == null) {
                        str2 = "";
                    }
                    result = str1.compareTo(str2);
                }
                return result;
            }
        };

        List<File> filesList = sortInBatch(new File(inputfile), comparator, maxtmpfiles, cs);
        mergeSortedFiles(filesList, new File(outputfile), comparator, cs);
    }

    // we divide the file into small blocks. If the blocks
    // are too small, we shall create too many temporary files.
    // If they are too big, we shall be using too much memory.
    public static long estimateBestSizeOfBlocks(File filetobesorted, int maxtmpfiles) {
        long sizeoffile = filetobesorted.length() * 4;
        /**
         * We multiply by two because later on someone insisted on counting the
         * memory usage as 4 bytes per character. By this model, loading a file
         * with 1 character will use 4 bytes.
         */
        // we don't want to open up much more than maxtmpfiles temporary files,
        // better run out of memory first.
        long blocksize = sizeoffile / maxtmpfiles + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

        // on the other hand, we don't want to create many temporary files for naught.
        // If blocksize is smaller than half the free memory, grow it.
        long freemem = Runtime.getRuntime().freeMemory();
        if (blocksize < freemem / 2) {
            blocksize = freemem / 2;
        }
       
        return blocksize;
    }

    /**
     * This will simply load the file by blocks of x rows, then sort them
     * in-memory, and write the result to temporary files that have to be merged
     * later. You can specify a bound on the number of temporary files that will
     * be created.
     *
     * @param file
     *            some flat file
     * @param comparator
     *            string comparator
     * @param maxtmpfiles
     *            maximal number of temporary files
     * @param Charset
     *            character set to use (can use Charset.defaultCharset())
     * @return a list of temporary flat files
     */
    public static List<File> sortInBatch(File file, Comparator<List<String>> comparator, int maxtmpfiles, Charset cs) throws IOException {

        List<File> files = new ArrayList<File>();
        ObjectInputStream fbr = new ObjectInputStream(new BufferedInputStream(new SnappyInputStream(new FileInputStream(file)), BUFFER_SIZE));
        long blocksize = estimateBestSizeOfBlocks(file, maxtmpfiles);// in bytes

        try {
            List<List<String>> tmplist = new ArrayList<List<String>>();
            List<String> line = new ArrayList<String>();
            try {
                while (line != null) {
                    long currentblocksize = 0;// in bytes
                    while ((currentblocksize < blocksize) && (line = ((RowData) fbr.readObject()).getData()) != null) {

                        tmplist.add(line);
                        // ram usage estimation, not very accurate, still more
                        // realistic that the simple 2 * String.length
                        currentblocksize += estimatedSizeOf(line);
                    }
                    files.add(sortAndSave(tmplist, comparator, cs));
                    tmplist.clear();
                }
            } catch (EOFException oef) {
                if (tmplist.size() > 0) {
                    files.add(sortAndSave(tmplist, comparator, cs));
                    tmplist.clear();
                }
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        } finally {
            fbr.close();
        }
        return files;
    }

    /**
     * Sort a list and save it to a temporary file
     *
     * @return the file containing the sorted data
     * @param tmplist
     *            data to be sorted
     * @param cmp
     *            string comparator
     * @param cs
     *            charset to use for output (can use Charset.defaultCharset())
     */
    public static File sortAndSave(List<List<String>> tmplist, Comparator<List<String>> cmp, Charset cs) throws IOException {

        Collections.sort(tmplist, cmp);
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", null);
        newtmpfile.deleteOnExit();

        ObjectOutputStream fbw = new ObjectOutputStream(new BufferedOutputStream(new SnappyOutputStream(new FileOutputStream(newtmpfile)), BUFFER_SIZE));
        try {
            int writeCount = 0;
            for (List<String> r : tmplist) {
                fbw.writeObject(new RowData(r));
                writeCount++;

                if (writeCount % FLUSH_INTERVAL == 0) {
                    fbw.flush();
                    fbw.reset();
                    writeCount = 0;
                }
            }
        } finally {
            fbw.flush();
            fbw.reset();
            fbw.close();
        }
        return newtmpfile;
    }

    /**
     * This merges a bunch of temporary flat files
     *
     * @param files
     *            The {@link List} of sorted {@link File}s to be merged.
     * @param Charset
     *            character set to use to load the strings
     * @param outputfile
     *            The output {@link File} to merge the results to.
     * @param comparator
     *            The {@link Comparator} to use to compare {@link String}s.
     * @param cs
     *            The {@link Charset} to be used for the byte to character
     *            conversion.
     * @return The number of lines sorted. (P. Beaudoin)
     * @since v0.1.4
     */
    public static int mergeSortedFiles(List<File> files, File outputfile, final Comparator<List<String>> comparator, Charset cs) throws IOException {

        PriorityQueue<BinaryFileBuffer> pq = new PriorityQueue<BinaryFileBuffer>(
                11,
                new Comparator<BinaryFileBuffer>() {
                    @Override
                    public int compare(BinaryFileBuffer i, BinaryFileBuffer j) {
                        return comparator.compare(i.peek(), j.peek());
                    }
                });

        for (File f : files) {
            BinaryFileBuffer bfb = new BinaryFileBuffer(f, cs);
            pq.add(bfb);
        }

        ObjectOutputStream fbw = new ObjectOutputStream(new BufferedOutputStream(new SnappyOutputStream(new FileOutputStream(outputfile, false)), BUFFER_SIZE));
        int rowcounter = 0;
        try {
            int writeCount = 0;
            while (pq.size() > 0) {
                BinaryFileBuffer bfb = pq.poll();
                List<String> r = bfb.pop();
                fbw.writeObject(new RowData(r));

                writeCount++;
                if (writeCount % FLUSH_INTERVAL == 0) {
                    fbw.flush();
                    fbw.reset();
                    writeCount = 0;
                }

                ++rowcounter;
                if (bfb.empty()) {
                    bfb.fbr.close();
                    bfb.originalfile.delete();// we don't need you anymore
                } else {
                    pq.add(bfb); // add it back
                }
            }
        } finally {
            fbw.flush();
            fbw.reset();
            fbw.close();
            for (BinaryFileBuffer bfb : pq) {
                bfb.close();
            }
        }

        return rowcounter;
    }

}

class BinaryFileBuffer {
    public ObjectInputStream fbr;
    public File originalfile;
    private List<String> cache;
    private boolean empty;

    public static final int BUFFER_SIZE = 2048;

    public BinaryFileBuffer(File f, Charset cs) throws IOException {
        this.originalfile = f;
        this.fbr = new ObjectInputStream(new BufferedInputStream(new SnappyInputStream(new FileInputStream(f)), BUFFER_SIZE));
        reload();
    }

    public boolean empty() {
        return this.empty;
    }

    private void reload() throws IOException {
        try {
            if ((this.cache = ((RowData) this.fbr.readObject()).getData()) == null) {
                this.empty = true;
                this.cache = null;
            } else {
                this.empty = false;
            }
        } catch (EOFException oef) {
            this.empty = true;
            this.cache = null;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public List<String> peek() {
        if (empty()) {
            return null;
        }
        return this.cache;
    }

    public List<String> pop() throws IOException {
        List<String> answer = peek();
        reload();
        return answer;
    }

}