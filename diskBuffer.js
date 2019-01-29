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
const fs = __importStar(require("async-file"));
const path = __importStar(require("path"));
const cancelToken_1 = require("./cancelToken");
function canWriteToFile(outputFile, maxSizeBytes) {
    return __awaiter(this, void 0, void 0, function* () {
        let exists = yield fs.exists(outputFile);
        if (!exists)
            return true;
        let remainingSize = maxSizeBytes - (yield fs.stat(outputFile)).size;
        if (remainingSize > 0)
            return true;
        return false;
    });
}
function getNextFile(outputFile, oldFile) {
    let oldFilename = path.basename(oldFile);
    let baseFilename = path.basename(outputFile);
    let folder = oldFile.substr(0, oldFile.length - oldFilename.length);
    let extension = path.extname(oldFile);
    let oldFileWithoutExtension = oldFilename.substr(0, oldFilename.length - extension.length);
    let baseFileWithoutExtension = baseFilename.substr(0, baseFilename.length - extension.length);
    let suffix = oldFileWithoutExtension.substr(baseFileWithoutExtension.length + 1);
    let nr = parseInt(suffix.substr(0, suffix.length - 1));
    nr++;
    return path.join(folder, baseFileWithoutExtension + "(" + nr + ")" + extension);
}
function getLatestFile(outputFile, maxSizeBytes) {
    return __awaiter(this, void 0, void 0, function* () {
        let curFile;
        let filename = path.basename(outputFile);
        let folder = outputFile.substr(0, outputFile.length - filename.length);
        let extension = path.extname(outputFile);
        let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
        let currentFiles = yield fs.readdir(folder);
        let matchingFiles = currentFiles.filter(file => file.startsWith(fileWithoutExtension));
        // parse the max counter
        let maxCounter = 0;
        for (let i = 0; i < matchingFiles.length; i++) {
            let suffix = matchingFiles[i].substr(fileWithoutExtension.length + 1);
            let nr = suffix.substr(0, suffix.length - extension.length - 1);
            if (parseInt(nr) > maxCounter)
                maxCounter = parseInt(nr);
        }
        let counter = maxCounter;
        do {
            // make curFile unique
            curFile = path.join(folder, fileWithoutExtension + "(" + counter + ")" + extension);
            counter++;
        } while (!(yield canWriteToFile(curFile, maxSizeBytes)));
        return curFile;
    });
}
function writeToStream(stream, chunk) {
    return new Promise((then, reject) => {
        try {
            stream.write(chunk, (err) => {
                if (err)
                    then(err);
                else
                    then(null);
            });
        }
        catch (e) {
            reject(e);
        }
    });
}
function openStream(stream) {
    return new Promise((then, reject) => {
        stream.once("open", fd => {
            then(null);
        });
        stream.once("error", err => {
            reject(err);
        });
    });
}
function removeOldestFiles(outputFile, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function* () {
        let filename = path.basename(outputFile);
        let folder = outputFile.substr(0, outputFile.length - filename.length);
        let extension = path.extname(outputFile);
        let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
        let currentFiles = yield fs.readdir(folder);
        let matchingFilesNrs = currentFiles.filter(file => file.startsWith(fileWithoutExtension))
            .map(file => {
            let suffix = file.substr(fileWithoutExtension.length + 1);
            let nr = suffix.substr(0, suffix.length - extension.length - 1);
            return parseInt(nr);
        });
        if (matchingFilesNrs.length > keepMaxFiles) {
            let sortedNrs = matchingFilesNrs.sort((a, b) => a - b);
            for (let i = 0; i < sortedNrs.length - keepMaxFiles; i++) {
                let filepath = path.join(folder, fileWithoutExtension + "(" + sortedNrs[i] + ")" + extension);
                yield fs.delete(filepath);
            }
        }
    });
}
function isBufferOverflow(outputFile, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function* () {
        let filename = path.basename(outputFile);
        let folder = outputFile.substr(0, outputFile.length - filename.length);
        let extension = path.extname(outputFile);
        let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
        let currentFiles = yield fs.readdir(folder);
        let matchingFilesNrs = currentFiles.filter(file => file.startsWith(fileWithoutExtension))
            .map(file => {
            let suffix = file.substr(fileWithoutExtension.length + 1);
            let nr = suffix.substr(0, suffix.length - extension.length - 1);
            return parseInt(nr);
        });
        if (matchingFilesNrs.length > keepMaxFiles) {
            return true;
        }
        return false;
    });
}
function pipeStreamToFiles(stream, outputFile, maxSizeBytes, cancelToken, keepMaxFiles, throwErrorOnBufferOverflow = true) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((then, reject) => __awaiter(this, void 0, void 0, function* () {
            let curFile = yield getLatestFile(outputFile, maxSizeBytes);
            let writeStream = fs.createWriteStream(curFile, {
                flags: "a"
            });
            yield openStream(writeStream);
            let dataCounter = (yield fs.stat(curFile)).size;
            stream.on("data", (chunk) => __awaiter(this, void 0, void 0, function* () {
                stream.pause();
                if (dataCounter + chunk.length > maxSizeBytes) {
                    let remainderSize = maxSizeBytes - dataCounter;
                    if (remainderSize > 0) {
                        let remainder = chunk.slice(0, remainderSize);
                        let err = yield writeToStream(writeStream, remainder);
                        if (err)
                            console.error("Error writing to file " + curFile + ": " + err);
                        chunk = chunk.slice(remainderSize);
                    }
                    // close the current file and  create a new one
                    writeStream.close();
                    curFile = getNextFile(outputFile, curFile);
                    writeStream = fs.createWriteStream(curFile);
                    yield openStream(writeStream);
                    if (keepMaxFiles) {
                        // check if the nr of files in the folder doesn't exceed the max limit
                        if (throwErrorOnBufferOverflow) {
                            let bufferOverflow = yield isBufferOverflow(outputFile, keepMaxFiles);
                            if (bufferOverflow) {
                                reject(new Error("Buffer overflow"));
                                ;
                            }
                        }
                        else {
                            removeOldestFiles(outputFile, keepMaxFiles);
                        }
                    }
                    dataCounter = 0;
                }
                let err = yield writeToStream(writeStream, chunk);
                if (err)
                    console.error("Error writing to file " + curFile + ": " + err);
                dataCounter += chunk.length;
                // don't attempt to read further if cancelled, abandon stream
                if (cancelToken.isCancelled()) {
                    writeStream.close();
                    then();
                    return;
                }
                stream.resume();
            }));
            stream.on("end", () => {
                writeStream.close();
                then();
            });
            stream.on("close", () => {
                writeStream.close();
                then();
            });
        }));
    });
}
function pipeFileToStream(file, outStream, expectedChunkSize, offset, nrOfBytesToRead) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((then, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                let readStream = fs.createReadStream(file, {
                    flags: "r",
                    start: offset,
                    end: offset + nrOfBytesToRead // must read until a specific point and not EOF or else read chunks will get corrupted and skip a bunch of bytes occassionally (probably due to race conditions of being written to at the same time)
                });
                let dataCounter = offset;
                yield openStream(readStream);
                let isFinished = false;
                readStream.on("data", (chunk) => __awaiter(this, void 0, void 0, function* () {
                    // make a hard copy to see if this helps against the corruption
                    let cloneBuffer = Buffer.alloc(chunk.length);
                    chunk.copy(cloneBuffer);
                    chunk = cloneBuffer;
                    readStream.pause();
                    if (dataCounter + chunk.length >= expectedChunkSize) {
                        isFinished = true;
                    }
                    //  console.log(`Piped data chunk of ${chunk.length} bytes, range: ${dataCounter} --> ${dataCounter + chunk.length}`);
                    dataCounter += chunk.length;
                    yield writeToStream(outStream, chunk);
                    //  console.log("Resuming stream");
                    readStream.resume();
                }));
                readStream.on("end", () => {
                    // we're done reading
                    //console.log(`Done reading from file, data counter is at ${dataCounter}`)
                    readStream.close();
                    then({
                        bytesWritten: dataCounter,
                        isFinished: isFinished
                    });
                });
            }
            catch (e) {
                reject(e);
            }
        }));
    });
}
function delay(ms) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((then, reject) => {
            setTimeout(then, ms);
        });
    });
}
function pipeFilesToStream(inputFile, outStream, expectedChunkSize, cancelToken) {
    return __awaiter(this, void 0, void 0, function* () {
        let stop = false;
        outStream.on("close", () => {
            console.log("Output stream was closed");
            stop = true;
        });
        let globalBytesWrittenToOutputStream = 0;
        while (!stop && !cancelToken.isCancelled()) {
            let files = [];
            let filename = path.basename(inputFile);
            let folder = inputFile.substr(0, inputFile.length - filename.length);
            let extension = path.extname(inputFile);
            let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
            let currentFiles = yield fs.readdir(folder);
            let matchingFilesNrs = currentFiles.filter(file => file.startsWith(fileWithoutExtension))
                .map(file => {
                let suffix = file.substr(fileWithoutExtension.length + 1);
                let nr = suffix.substr(0, suffix.length - extension.length - 1);
                return parseInt(nr);
            });
            let sortedNrs = matchingFilesNrs.sort((a, b) => a - b);
            for (let i = 0; i < sortedNrs.length; i++) {
                let filepath = path.join(folder, fileWithoutExtension + "(" + sortedNrs[i] + ")" + extension);
                files.push(filepath);
            }
            while (files.length > 0) {
                let file = files.shift();
                let isFinished = false;
                let dataOffset = 0;
                while (!isFinished && !cancelToken.isCancelled()) {
                    let fileSize = (yield fs.stat(file)).size;
                    if (dataOffset < fileSize) {
                        //console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                        let nrOfBytesToRead = fileSize - dataOffset;
                        let result = yield pipeFileToStream(file, outStream, expectedChunkSize, dataOffset, nrOfBytesToRead);
                        globalBytesWrittenToOutputStream += result.bytesWritten - dataOffset;
                        console.log(`Written ${result.bytesWritten - dataOffset} bytes from ${file} with offset ${dataOffset}, total bytes sent to outputstream is ${globalBytesWrittenToOutputStream}`);
                        isFinished = result.isFinished;
                        dataOffset = result.bytesWritten;
                    }
                    if (!isFinished) {
                        // console.warn("Buffer underrun, waiting for data to become available");
                        yield delay(100); //  wait a bit for data to become available
                    }
                }
                // now clean it up
                yield fs.delete(file);
            }
            // prevent hammering with tight loop
            yield delay(100);
        }
    });
}
class DiskBufferWriter {
    constructor(pathAndFileFormat, chunkSize, keepMaxFiles) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.chunkSize = chunkSize;
        this.keepMaxFiles = keepMaxFiles;
        this.cancelToken = new cancelToken_1.CancelToken();
    }
    pipeToDisk(readableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            yield pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.cancelToken, this.keepMaxFiles);
        });
    }
    cancel() {
        return __awaiter(this, void 0, void 0, function* () {
            this.cancelToken.cancel();
        });
    }
}
exports.DiskBufferWriter = DiskBufferWriter;
const deleteFolderRecursive = (p) => __awaiter(this, void 0, void 0, function* () {
    if (yield fs.exists(p)) {
        for (let entry of yield fs.readdir(p)) {
            const curPath = path + "/" + entry;
            if ((yield fs.lstat(curPath)).isDirectory())
                yield deleteFolderRecursive(curPath);
            else
                yield fs.unlink(curPath);
        }
        yield fs.rmdir(p);
    }
});
class DiskBufferReader {
    constructor(pathAndFileFormat, expectedChunkSize) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.expectedChunkSize = expectedChunkSize;
        this.cancelToken = new cancelToken_1.CancelToken();
    }
    pipeFromDisk(writeableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            yield pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize, this.cancelToken);
        });
    }
    cleanUp() {
        return __awaiter(this, void 0, void 0, function* () {
            let filename = path.basename(this.pathAndFileFormat);
            let folder = this.pathAndFileFormat.substr(0, this.pathAndFileFormat.length - filename.length);
            console.log("Cleaning up disk buffer, deleting folder " + folder);
            yield deleteFolderRecursive(folder);
        });
    }
    closeWhenBufferIsDone() {
        return __awaiter(this, void 0, void 0, function* () {
            this.cancelToken.cancel();
        });
    }
}
exports.DiskBufferReader = DiskBufferReader;
//# sourceMappingURL=diskBuffer.js.map