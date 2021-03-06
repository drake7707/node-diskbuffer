import { Stream } from "stream";
import * as diskBuffer from "./diskBuffer";
import * as mp4DiskBuffer from "./mp4DiskBuffer";

import * as fs from "fs"






class IncrementStream extends Stream.Readable {
    _read() {
        let arr: number[] = [];
        for (let i = 0; i < 256; i++) {
            arr.push(i);
        }
        let buf = Buffer.from(arr);
        this.push(buf);
    }
}
class PrintStream extends Stream.Writable {

    private lastNr = -1;

    _write(chunk: Buffer, encoding: string, done: Function) {
        for (let i = 0; i < chunk.length; i++) {
            let nr = chunk.readUInt8(i);

            if ((nr == 0 && this.lastNr == 255) || (nr - this.lastNr == 1)) {
                // ok expected
            } else {
                console.error("Invalid transition: " + this.lastNr + " -> " + nr);
            }
            this.lastNr = nr;

            //process.stdout.write(nr + " ");
        }
        console.log("Printed " + chunk.length + " nrs");
        done();
    }
}


async function main() {

    let incStream = fs.createReadStream("C:\\Custom\\postedvideo-1.mp4", { highWaterMark: 500 });
    let printStream = fs.createWriteStream("C:\\Custom\\testvod-out.mp4", { flags: "w" });

    //    incStream.pipe(printStream);

    let chunkSize = 20 * 1024 * 1024;
    let testfile = "C:\\Custom\\tempstorage\\testfile.chk";
    let keepMaxFiles = 500;

    let writer = new diskBuffer.DiskBufferWriter(testfile, chunkSize, keepMaxFiles);

    writer.pipeToDisk(incStream)
        .catch(err => {
            console.log("Error writing to disk: " + err);
            process.exit(1);
        });


    
let reader = new diskBuffer.DiskBufferReader(testfile, chunkSize);
reader.pipeFromDisk(printStream)
    .catch(err => {
        console.log("Error reading from disk: " + err);
        process.exit(2);
    });


   /* setTimeout(() => {
        reader.closeWhenBufferIsDone();
    }, 1000);*/
}


main().then(() => {

}).catch((err) => {
    console.error(err);
})


