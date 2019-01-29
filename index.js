"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
const stream_1 = require("stream");
const diskBuffer = __importStar(require("./diskBuffer"));
const fs = __importStar(require("fs"));
class IncrementStream extends stream_1.Stream.Readable {
    _read() {
        let arr = [];
        for (let i = 0; i < 256; i++) {
            arr.push(i);
        }
        let buf = Buffer.from(arr);
        this.push(buf);
    }
}
class PrintStream extends stream_1.Stream.Writable {
    constructor() {
        super(...arguments);
        this.lastNr = -1;
    }
    _write(chunk, encoding, done) {
        for (let i = 0; i < chunk.length; i++) {
            let nr = chunk.readUInt8(i);
            if ((nr == 0 && this.lastNr == 255) || (nr - this.lastNr == 1)) {
                // ok expected
            }
            else {
                console.error("Invalid transition: " + this.lastNr + " -> " + nr);
            }
            this.lastNr = nr;
            //process.stdout.write(nr + " ");
        }
        console.log("Printed " + chunk.length + " nrs");
        done();
    }
}
function main() {
    return __awaiter(this, void 0, void 0, function* () {
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
    });
}
main().then(() => {
}).catch((err) => {
    console.error(err);
});
//# sourceMappingURL=index.js.map