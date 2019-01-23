"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
var stream_1 = require("stream");
var mp4DiskBuffer = __importStar(require("./mp4DiskBuffer"));
var fs = __importStar(require("fs"));
var IncrementStream = /** @class */ (function (_super) {
    __extends(IncrementStream, _super);
    function IncrementStream() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    IncrementStream.prototype._read = function () {
        var arr = [];
        for (var i = 0; i < 256; i++) {
            arr.push(i);
        }
        var buf = Buffer.from(arr);
        this.push(buf);
    };
    return IncrementStream;
}(stream_1.Stream.Readable));
var PrintStream = /** @class */ (function (_super) {
    __extends(PrintStream, _super);
    function PrintStream() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.lastNr = -1;
        return _this;
    }
    PrintStream.prototype._write = function (chunk, encoding, done) {
        for (var i = 0; i < chunk.length; i++) {
            var nr = chunk.readUInt8(i);
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
    };
    return PrintStream;
}(stream_1.Stream.Writable));
function main() {
    var incStream = fs.createReadStream("C:\\Custom\\postedvideo-1.mp4");
    var printStream = fs.createWriteStream("C:\\Custom\\testvod-out.mp4", { flags: "w" });
    //    incStream.pipe(printStream);
    var chunkSize = 4 * 1024 * 1024;
    var testfile = "C:\\Custom\\tempstorage\\testfile.chk";
    var keepMaxFiles = 500;
    var writer = new mp4DiskBuffer.DiskBufferWriter(testfile, chunkSize, keepMaxFiles);
    /*writer.pipeToDisk(incStream)
        .catch(err => {
            console.log("Error writing to disk: " + err);
            process.exit(1);
        });

*/
    var reader = new mp4DiskBuffer.DiskBufferReader(testfile, chunkSize, true);
    reader.pipeFromDisk(printStream)
        .catch(function (err) {
        console.log("Error reading from disk: " + err);
        process.exit(2);
    });
}
main();
//# sourceMappingURL=index.js.map