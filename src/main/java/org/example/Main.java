package org.example;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws IOException {
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        int MAX_RECORDS = 100;
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(111);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));

        for (int i = 0 ; i < MAX_RECORDS ; i++) {
            User user = new User();
            user.setName("Alyssa" + i);
            user.setFavoriteNumber(111);
            dataFileWriter.append(user);
        }



//        Storage storage = StorageOptions.getDefaultInstance().getService();
//
//        // The name for the new bucket
//        String bucketName = "test_avro_zcsong"; // "my-new-bucket";
//        String objectName = "users.avro";
//        BlobId blobId = BlobId.of(bucketName, objectName);
//        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
//        Blob blob = storage.get("test_avro_zcsong", "users.avro");
//
//        try (ReadChannel reader = storage.reader(BlobId.of(bucketName, objectName));) {
//
//             InputStream readerStream = Channels.newInputStream(reader);
//             DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
//             DataFileStream<User> dataFileStream = new DataFileStream<User>(readerStream, userDatumReader);
//             int count = 0;
//             while (dataFileStream.hasNext()) {
//                 count ++;
//                 if (count % 1000000 == 0) {
//                     System.out.println("Read " + count + " users");
//                 }
//                 User readline = dataFileStream.next();
//             }
//        }

//        // SHOULD EXPLODE
//          try (ReadChannel reader = storage.reader(BlobId.of(bucketName, objectName));) {
//             InputStream readerStream = Channels.newInputStream(reader);
//             DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
//             DataFileReader<User> dataFileStream = new DataFileReader<User>(new SeekableByteArrayInput(readerStream.readAllBytes()), userDatumReader);
//             while (dataFileStream.hasNext()) {
//                 User readline = dataFileStream.next();
//                 System.out.println(readline);
//             }
//        }

    }
}