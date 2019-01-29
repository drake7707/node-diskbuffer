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
const CancelToken_1 = require("./CancelToken");
exports.DEBUG = false;
function log(msg) {
    if (exports.DEBUG)
        console.log(msg);
}
function error(msg) {
    if (exports.DEBUG)
        console.error(msg);
}
function canWriteToFile(outputFile, maxSizeBytes) {
    return __awaiter(this, void 0, void 0, function* () {
        let exists = yield fs.exists(outputFile);
        if (!exists)
            return true;
        let fileSize = (yield fs.stat(outputFile)).size;
        let remainingSize = maxSizeBytes - fileSize;
        if (remainingSize > 0)
            return true;
        // it's possible that the mp4 atom needs to be finished so
        // remaining size depends on maxSizeBytes + length of mp4offset
        let expectedFileLength = yield getExpectedFileLengthForCorrectMP4AtomAlignment(outputFile);
        if (expectedFileLength < fileSize || expectedFileLength > fileSize) {
            // file needs some work (either truncate or append with padding)
            return true;
        }
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
function getInitFilePath(outputFile) {
    let curFile;
    let filename = path.basename(outputFile);
    let folder = outputFile.substr(0, outputFile.length - filename.length);
    let extension = path.extname(outputFile);
    let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
    return path.join(folder, "init" + extension);
}
function getLatestFilePath(outputFile, maxSizeBytes) {
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
function getExpectedFileLengthForCorrectMP4AtomAlignment(filepath) {
    return __awaiter(this, void 0, void 0, function* () {
        let stream = fs.createReadStream(filepath);
        yield openStream(stream);
        let result = yield new Promise((then, reject) => {
            let mp4AtomOffset = 0;
            let totalDataLength = 0;
            let partialAtomBuffer = null;
            stream.on("data", (buffer) => __awaiter(this, void 0, void 0, function* () {
                if (partialAtomBuffer != null) {
                    // there was a partial atom length+ atom offset
                    // prepend it to the buffer
                    buffer = Buffer.concat([partialAtomBuffer, buffer]);
                    partialAtomBuffer = null;
                }
                while (mp4AtomOffset + 4 < totalDataLength + buffer.length) {
                    let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                    let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                    mp4AtomOffset += mp4AtomLength;
                }
                if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 4 >= totalDataLength + buffer.length) {
                    // the atom length/atom is fragmented over this buffer and the next one
                    // truncate the buffer to remove the partial atom, but store it in a variable
                    // first, so it can be prepended for the next buffer
                    let relativeOffset = mp4AtomOffset - totalDataLength;
                    partialAtomBuffer = buffer.slice(relativeOffset);
                    buffer = buffer.slice(0, relativeOffset);
                }
                totalDataLength += buffer.length;
            }));
            stream.on("end", () => {
                // mp4AtomOffset will be the position when the next mp4 atom should appear
                if (partialAtomBuffer != null) {
                    // oh no the atom was fragmented
                    // need to truncate the file to last mp4 atom
                    then(mp4AtomOffset);
                }
                else {
                    then(mp4AtomOffset);
                }
            });
        });
        return result;
    });
}
function pipeStreamToFiles(stream, outputFile, maxSizeBytes, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function* () {
        let curFile = yield getLatestFilePath(outputFile, maxSizeBytes);
        let currentChunkFileSize = (!(yield fs.exists(curFile))) ? 0 : (yield fs.stat(curFile)).size;
        if (currentChunkFileSize > 0) {
            // there was already an existing chunk file that needs to be continued
            // the file might not be aligned on a correct mp4 atom 
            let chunkFileShouldBeOfLength = yield getExpectedFileLengthForCorrectMP4AtomAlignment(curFile);
            if (currentChunkFileSize < chunkFileShouldBeOfLength) {
                // pad with zeroes
                log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, padding with zeroes");
                let padding = new Array(chunkFileShouldBeOfLength - currentChunkFileSize).fill(0);
                yield fs.appendFile(curFile, Buffer.from(padding));
            }
            else if (currentChunkFileSize > chunkFileShouldBeOfLength) {
                // file needs to be truncated to align the proper mp4 atom
                log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, truncating to last expected mp4 atom position");
                yield fs.truncate(curFile, chunkFileShouldBeOfLength);
            }
            else {
                // hey it perfectly aligns, nothing needs to be done
                log("Continuing existing chunk " + curFile);
            }
        }
        let writeStream = fs.createWriteStream(curFile, {
            flags: "a"
        });
        yield openStream(writeStream);
        let totalDataLength = 0;
        let mp4AtomOffset = 0;
        // track the offset of the last moof. Chunks should not only be mp4 atom aligned, they should only split on moof
        // because [ftyp+moov]+mdat is invalid
        let moofMP4AtomOffset = -1;
        let initBuffer = Buffer.from([]);
        let initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
        let initBufferProcessingAtom = "";
        let partialAtomBuffer = null;
        stream.on("data", (buffer) => __awaiter(this, void 0, void 0, function* () {
            stream.pause();
            //log("Data buffer received " + totalDataLength + " -> " + (totalDataLength + buffer.length));
            // ----- CHECK: Atom was fragmented over multiple data events?
            if (partialAtomBuffer != null) {
                // there was a partial atom length+ atom offset
                // prepend it to the buffer
                buffer = Buffer.concat([partialAtomBuffer, buffer]);
                partialAtomBuffer = null;
            }
            // ------ CHECK: Things to write to init buffer was fragmented over multiple data events?
            if (initBufferNeedsToBeAppendedUntilNextMP4Atom) {
                // we were reading an atom that needs to be appended to the init buffer
                // but it was fragmented over the buffer
                // continue reading until the next atom
                let nextRelativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                if (nextRelativeMP4AtomOffsetInBuffer < buffer.length) {
                    initBuffer = Buffer.concat([initBuffer, buffer.slice(0, nextRelativeMP4AtomOffsetInBuffer)]);
                    initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                }
                else {
                    // the part is not complete, need to continue reading on the next data event
                    initBuffer = Buffer.concat([initBuffer, buffer.slice(0, nextRelativeMP4AtomOffsetInBuffer)]);
                    initBufferNeedsToBeAppendedUntilNextMP4Atom = true;
                }
                // if the atom we're processing is a moov and it's fully read, then finish it up and write the init file
                if (!initBufferNeedsToBeAppendedUntilNextMP4Atom && initBufferProcessingAtom == "moov") {
                    // finished the moov atom, write the init file to a separate file
                    let initPath = getInitFilePath(outputFile);
                    yield fs.writeFile(initPath, initBuffer);
                    initBuffer = Buffer.from([]);
                }
                // clear, the atom is complete
                if (!initBufferNeedsToBeAppendedUntilNextMP4Atom)
                    initBufferProcessingAtom = "";
            }
            // --- ADVANCE mp4 atom and extract ftyp and moov atoms to push to the init buffer
            let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
            // ---- READ MP4 Atoms to track mp4AtomOffset and extract the ftyp+moov for init 
            // make sure all atom length + atom is actually within the buffer (so do +8)
            while (mp4AtomOffset + 8 < totalDataLength + buffer.length) {
                relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                let atom = buffer.toString("utf8", relativeMP4AtomOffsetInBuffer + 4, relativeMP4AtomOffsetInBuffer + 4 + 4);
                log("mp4 atom '" + atom + "' offset: " + mp4AtomOffset + " -> " + (mp4AtomOffset + mp4AtomLength));
                // --- extract ftyp+moov to the init buffer and write init chunk if complete
                if (atom == "ftyp") {
                    initBufferProcessingAtom = atom;
                    // mp4 file header, start of mp4 file
                    if (relativeMP4AtomOffsetInBuffer + mp4AtomLength < buffer.length) {
                        initBuffer = buffer.slice(relativeMP4AtomOffsetInBuffer, relativeMP4AtomOffsetInBuffer + mp4AtomLength);
                        initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                    }
                    else {
                        // the part is not complete, need to continue reading on the next data event
                        initBuffer = buffer.slice(relativeMP4AtomOffsetInBuffer);
                        initBufferNeedsToBeAppendedUntilNextMP4Atom = true;
                    }
                }
                else if (atom == "moov") {
                    initBufferProcessingAtom = atom;
                    if (relativeMP4AtomOffsetInBuffer + mp4AtomLength < buffer.length) {
                        initBuffer = Buffer.concat([initBuffer, buffer.slice(relativeMP4AtomOffsetInBuffer, relativeMP4AtomOffsetInBuffer + mp4AtomLength)]);
                        // finished the moov atom, write the init file to a separate file
                        let initPath = getInitFilePath(outputFile);
                        yield fs.writeFile(initPath, initBuffer);
                        initBuffer = Buffer.from([]);
                        initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                    }
                    else {
                        // the part is not complete, need to continue reading on the next data event
                        initBuffer = Buffer.concat([initBuffer, buffer.slice(relativeMP4AtomOffsetInBuffer)]);
                        initBufferNeedsToBeAppendedUntilNextMP4Atom = true;
                    }
                }
                if (atom == "moof") {
                    moofMP4AtomOffset = mp4AtomOffset;
                }
                mp4AtomOffset += mp4AtomLength;
            }
            if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 8 >= totalDataLength + buffer.length) {
                // the atom length/atom is fragmented over this buffer and the next one
                // truncate the buffer to remove the partial atom, but store it in a variable
                // first, so it can be prepended for the next buffer
                let relativeOffset = mp4AtomOffset - totalDataLength;
                partialAtomBuffer = buffer.slice(relativeOffset);
                buffer = buffer.slice(0, relativeOffset);
                // TODO wait a minute, if we're changing the buffer, what impact does this have 
                // on all the offsets we're using here????
            }
            let relativeMOOFMP4AtomOffsetInBuffer = moofMP4AtomOffset - totalDataLength;
            // keep track of the total data length in the entire stream
            totalDataLength += buffer.length;
            if (currentChunkFileSize + buffer.length >= maxSizeBytes) {
                if (relativeMOOFMP4AtomOffsetInBuffer < 0) {
                    // wait until we have a moof atom in the buffer before splitting
                }
                else if (relativeMOOFMP4AtomOffsetInBuffer >= buffer.length) {
                    // this can't happen because the MOOF should be encountered in the buffer
                }
                else if (currentChunkFileSize + relativeMOOFMP4AtomOffsetInBuffer < maxSizeBytes) {
                    // the mp4 atom is in a position in the buffer where if it were to be cut until that point
                    // the total file size of the chunk would be below the expected file size. This may not happen
                    // or the reader will stall expecting the rest
                    // wait until the next data buffer before splitting
                }
                else {
                    // split on MOOF atom. This ensures that a chunk will always be
                    // ftyp+moov+moof+mdat, which is required for ffmpeg.
                    let remainderSize = relativeMOOFMP4AtomOffsetInBuffer;
                    if (remainderSize > 0) {
                        let remainder = buffer.slice(0, remainderSize);
                        let err = yield writeToStream(writeStream, remainder);
                        if (err)
                            error("Error writing to file " + curFile + ": " + err);
                        buffer = buffer.slice(remainderSize);
                    }
                    // close the current file and  create a new one
                    writeStream.close();
                    curFile = getNextFile(outputFile, curFile);
                    writeStream = fs.createWriteStream(curFile);
                    yield openStream(writeStream);
                    if (keepMaxFiles) {
                        // check if the nr of files in the folder doesn't exceed the max limit
                        yield removeOldestFiles(outputFile, keepMaxFiles);
                    }
                    currentChunkFileSize = 0;
                }
                /*  OLD CODE THAT SPLIT ON MP4 ATOM ALIGNED CHUNKS, NOT ON MOOF
                
                   // relativeMP4AtomOffsetInBuffer is now the last mp4 offset in the buffer
                   if (relativeMP4AtomOffsetInBuffer > buffer.length) {
                       // it's beyond the buffer length, wait for the next data buffer before splitting
                   }
                   else if (currentChunkFileSize + relativeMP4AtomOffsetInBuffer < maxSizeBytes) {
                       // the mp4 atom is in a position in the buffer where if it were to be cut until that poin
                       // the total file size of the chunk would be below the expected file size. This may not happen
                       // or the reader will stall expecting the rest
                       // wait until the next data buffer before splitting
                   }
                   else {
                       // rather than take the remainder size on max size bytes, take the remainder on the last mp4 atom offset
                       //let remainderSize = maxSizeBytes - dataCounter;
                       let remainderSize = relativeMP4AtomOffsetInBuffer;
                       if (remainderSize > 0) {
                           let remainder = buffer.slice(0, remainderSize);
                           let err = await writeToStream(writeStream, remainder);
                           if (err)
                               error("Error writing to file " + curFile + ": " + err);
                           buffer = buffer.slice(remainderSize);
                       }
       
                       // close the current file and  create a new one
                       writeStream.close();
       
                       curFile = getNextFile(outputFile, curFile);
                       writeStream = fs.createWriteStream(curFile);
                       await openStream(writeStream);
       
                       if (keepMaxFiles) {
                           // check if the nr of files in the folder doesn't exceed the max limit
                           await removeOldestFiles(outputFile, keepMaxFiles);
                       }
                       currentChunkFileSize = 0;
                   }*/
            }
            let err = yield writeToStream(writeStream, buffer);
            if (err)
                error("Error writing to file " + curFile + ": " + err);
            currentChunkFileSize += buffer.length;
            stream.resume();
        }));
        stream.on("end", () => {
            writeStream.close();
        });
        stream.on("close", () => {
            writeStream.close();
        });
    });
}
function pipeFileToStream(file, outStream, expectedChunkSize, offset, nrOfBytesToRead, startMP4AtomOffset = 0) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((then, reject) => __awaiter(this, void 0, void 0, function* () {
            try {
                let curPositionInFile = offset;
                let readStream = fs.createReadStream(file, {
                    flags: "r",
                    start: curPositionInFile,
                    end: curPositionInFile + nrOfBytesToRead // must read until a specific point and not EOF or else read chunks will get corrupted and skip a bunch of bytes occassionally (probably due to race conditions of being written to at the same time)
                    //  highWaterMark: 1024 // only read tiny amount to see how partial atoms work fine -> they do!!!
                });
                yield openStream(readStream);
                let mp4AtomOffset = startMP4AtomOffset;
                let isFinished = false;
                let partialAtomBuffer = null;
                readStream.on("data", (buffer) => __awaiter(this, void 0, void 0, function* () {
                    log(`Buffer read, length ${buffer.length}, file chunk ${curPositionInFile} -> ${curPositionInFile + buffer.length}`);
                    if (isFinished) // don't do anything anymore, is finished
                        return;
                    readStream.pause();
                    let originalBufferLength = buffer.length;
                    if (partialAtomBuffer != null) {
                        // there was a partial atom length+ atom offset
                        // prepend it to the buffer
                        log(`There was a partial atom buffer (length: ${partialAtomBuffer.length}) from the previous data event, prepending it to the buffer`);
                        buffer = Buffer.concat([partialAtomBuffer, buffer]);
                        partialAtomBuffer = null;
                    }
                    // the disk buffer writer ensures that mp4 atoms are not fragmented over chunks
                    // and each chunk starts with an mp4 atom 
                    // this means that the filesize will vary anywhere between expectedChunkSize and expectedChunkSize + remainder of mp4 atom length
                    while (mp4AtomOffset + 8 < curPositionInFile + buffer.length) {
                        let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - curPositionInFile;
                        let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                        let atom = buffer.toString("utf8", relativeMP4AtomOffsetInBuffer + 4, relativeMP4AtomOffsetInBuffer + 4 + 4);
                        log("mp4 atom '" + atom + "' offset: " + mp4AtomOffset + " -> " + (mp4AtomOffset + mp4AtomLength));
                        mp4AtomOffset += mp4AtomLength;
                    }
                    if (mp4AtomOffset < curPositionInFile + buffer.length && mp4AtomOffset + 8 >= curPositionInFile + buffer.length) {
                        // again a partial scenario, there WILL be a next buffer containing the remainder of the mp4 atom offset
                        let relativeOffset = mp4AtomOffset - curPositionInFile;
                        log(`MP4 Atom is fragmented,  keeping the partial buffer seperate to append later, buffer length is ${buffer.length}, cur position in file is ${curPositionInFile}, mp4 atom offset is ${mp4AtomOffset}, relative offset in buffer is ${relativeOffset}`);
                        partialAtomBuffer = buffer.slice(relativeOffset);
                        buffer = buffer.slice(0, relativeOffset);
                        isFinished = false;
                    }
                    else {
                        if (curPositionInFile >= expectedChunkSize) {
                            // we've reached the expected chunk size, check if the latest mp4 atom is beyond this point
                            // the next mp4 atom is at mp4AtomOffset
                            log(`Reached expected chunk size (${curPositionInFile} > ${expectedChunkSize})`);
                            let realExpectedChunkSize = mp4AtomOffset;
                            if (curPositionInFile + buffer.length >= realExpectedChunkSize) {
                                log(`The current position + buffer length is longer or equal ${curPositionInFile} + ${buffer.length} >= ${realExpectedChunkSize} than the mp4 atom offset so the file is finished`);
                                isFinished = true;
                            }
                        }
                    }
                    // DO NOT WRITE ANYTHING AFTER THIS AWAIT THAT IS CHANGING  THINGS THAT IS REFERRED TO IN THE END EVENT!
                    // NODEJS IS UNDETERMINISTIC, SOMETIMES END IS CALLED BEFORE CONTINUING THE AWAIT (FOR SMALL BUFFERS)
                    log("Buffer written to output stream, advancing pointer");
                    // advance position _before_ the piping to the stream !, otherwise end event might get the wrong offset
                    curPositionInFile += buffer.length;
                    yield writeToStream(outStream, buffer);
                    // resume to get the next data event
                    readStream.resume();
                }));
                readStream.on("end", () => {
                    if (isFinished) {
                        // we're done reading
                        readStream.close();
                        then({
                            readUntilOffset: curPositionInFile,
                            nextMP4AtomOffset: mp4AtomOffset,
                            isFinished: true
                        });
                    }
                    else {
                        // still expecting more data in this file but it wasn't written yet
                        then({
                            readUntilOffset: curPositionInFile,
                            nextMP4AtomOffset: mp4AtomOffset,
                            isFinished: false
                        });
                    }
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
function fileStartsWithFtypAndMoov(inputFile) {
    return __awaiter(this, void 0, void 0, function* () {
        let stream = fs.createReadStream(inputFile);
        yield openStream(stream);
        return new Promise((then, reject) => {
            let mp4AtomOffset = 0;
            let totalDataLength = 0;
            let partialAtomBuffer = null;
            let atoms = [];
            stream.on("data", (buffer) => __awaiter(this, void 0, void 0, function* () {
                if (partialAtomBuffer != null) {
                    // there was a partial atom length+ atom offset
                    // prepend it to the buffer
                    buffer = Buffer.concat([partialAtomBuffer, buffer]);
                    partialAtomBuffer = null;
                }
                while (mp4AtomOffset + 8 < totalDataLength + buffer.length) {
                    let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                    let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                    let atom = buffer.toString("utf8", relativeMP4AtomOffsetInBuffer + 4, relativeMP4AtomOffsetInBuffer + 4 + 4);
                    atoms.push(atom);
                    mp4AtomOffset += mp4AtomLength;
                    if (atoms.length == 2) {
                        if (atoms[0] == "ftyp" && atoms[1] == "moov")
                            then({ hasInit: true, offsetAfterInit: mp4AtomOffset, notEnoughData: false });
                        else
                            then({ hasInit: false, offsetAfterInit: -1, notEnoughData: false });
                        stream.close();
                        return;
                    }
                }
                if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 8 >= totalDataLength + buffer.length) {
                    // the atom length/atom is fragmented over this buffer and the next one
                    // truncate the buffer to remove the partial atom, but store it in a variable
                    // first, so it can be prepended for the next buffer
                    let relativeOffset = mp4AtomOffset - totalDataLength;
                    partialAtomBuffer = buffer.slice(relativeOffset);
                    buffer = buffer.slice(0, relativeOffset);
                }
            }));
            stream.on("end", () => {
                if (atoms.length == 2) {
                    if (atoms[0] == "ftyp" && atoms[1] == "moov")
                        then({ hasInit: true, offsetAfterInit: mp4AtomOffset, notEnoughData: false });
                    else
                        then({ hasInit: false, offsetAfterInit: -1, notEnoughData: false });
                    stream.close();
                }
                else if (atoms.length < 2) {
                    // not enough data to read yet
                    then({ hasInit: false, offsetAfterInit: -1, notEnoughData: true });
                }
                else {
                    // read more than 2 atoms, but they don't start with ftyp and moov
                    then({ hasInit: false, offsetAfterInit: -1, notEnoughData: false });
                }
            });
        });
    });
}
function pipeFilesToStream(inputFile, outStream, expectedChunkSize, writeInitChunkIfAvailable, cancelToken) {
    return __awaiter(this, void 0, void 0, function* () {
        if (writeInitChunkIfAvailable) {
            let initFile = getInitFilePath(inputFile);
            log("Waiting for init file " + initFile + " to become available ");
            while (!(yield fs.exists(initFile)) && !cancelToken.isCancelled()) {
                yield delay(100);
            }
            if (cancelToken.isCancelled())
                return;
            if (yield fs.exists(initFile)) {
                yield new Promise((then, reject) => {
                    let initStream = fs.createReadStream(initFile);
                    initStream.on("data", buffer => {
                        outStream.write(buffer);
                    });
                    initStream.on("end", () => {
                        initStream.close();
                        then();
                    });
                });
            }
        }
        let outputStreamClosed = false;
        outStream.on("close", () => {
            log("Output stream was closed");
            outputStreamClosed = true;
        });
        log("Starting to read buffer files");
        while (!outputStreamClosed && !cancelToken.isCancelled()) {
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
            if (files.length > 0) {
                log(`There are ${files.length} to process`);
            }
            while (files.length > 0) {
                let file = files.shift();
                log(`Processing file ${file}`);
                yield processFileToPipeToStream(file, writeInitChunkIfAvailable, cancelToken, outStream, expectedChunkSize);
            }
            // prevent hammering with tight loop
            yield delay(100);
        }
    });
}
function processFileToPipeToStream(file, writeInitChunkIfAvailable, cancelToken, outStream, expectedChunkSize) {
    return __awaiter(this, void 0, void 0, function* () {
        let fileStillExists = yield fs.exists(file);
        if (!fileStillExists) {
            log(`File ${file} doesn't exist anymore, skipping file`);
            return;
        }
        let isFinished = false;
        let dataOffset = 0;
        let nextMP4AtomOffset = 0;
        if (dataOffset == 0 && writeInitChunkIfAvailable) {
            // must check if the file doesn't start with an init chunk
            let result = null;
            let hasEnoughData = false;
            while (!hasEnoughData && !cancelToken.isCancelled()) {
                fileStillExists = yield fs.exists(file);
                if (!fileStillExists) {
                    log(`File ${file} doesn't exist anymore, aborting check to see if the file starts with init atoms`);
                    return;
                }
                result = yield fileStartsWithFtypAndMoov(file);
                hasEnoughData = !result.notEnoughData;
                if (!hasEnoughData)
                    yield delay(100);
            }
            if (cancelToken.isCancelled()) {
                log("Reader was cancelled while waiting for init chunk to become available");
            }
            if (result != null && result.hasInit) {
                log(`File ${file} has init segment, the init chunk was already injected in the stream so skipping until after the init part to offset ${result.offsetAfterInit}`);
                dataOffset = result.offsetAfterInit;
                nextMP4AtomOffset = result.offsetAfterInit;
            }
            else {
                log(`File does not have an init segment, can just read from the beginning of the file`);
            }
        }
        while (!isFinished && !cancelToken.isCancelled()) {
            let fileSize = (yield fs.stat(file)).size;
            fileStillExists = yield fs.exists(file);
            if (!fileStillExists) {
                log(`File ${file} doesn't exist anymore, aborting pipe`);
                return;
            }
            if (dataOffset < fileSize) {
                log("Piping " + file + " with offset " + dataOffset + " to the stream");
                let nrOfBytesToRead = fileSize - dataOffset;
                let result = yield pipeFileToStream(file, outStream, expectedChunkSize, dataOffset, nrOfBytesToRead, nextMP4AtomOffset);
                log(`Data of ${file} was piped to the stream until offset ${result.readUntilOffset}, isFinished=${result.isFinished}, next mp4 offset is at ${result.nextMP4AtomOffset}`);
                isFinished = result.isFinished;
                dataOffset = result.readUntilOffset;
                nextMP4AtomOffset = result.nextMP4AtomOffset;
            }
            if (!isFinished) {
                //console.warn("Buffer underrun, waiting for data to become available");
                yield delay(100); //  wait a bit for data to become available
            }
        }
        // now clean it up
        log(`Cleaning up file ${file}`);
        yield fs.delete(file);
    });
}
class DiskBufferWriter {
    constructor(pathAndFileFormat, chunkSize, keepMaxFiles) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.chunkSize = chunkSize;
        this.keepMaxFiles = keepMaxFiles;
    }
    pipeToDisk(readableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            yield pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.keepMaxFiles);
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
    constructor(pathAndFileFormat, expectedChunkSize, writeInitChunkIfAvailable) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.expectedChunkSize = expectedChunkSize;
        this.writeInitChunkIfAvailable = writeInitChunkIfAvailable;
        this.cancelToken = new CancelToken_1.CancelToken();
    }
    pipeFromDisk(writeableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            yield pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize, this.writeInitChunkIfAvailable, this.cancelToken);
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
//# sourceMappingURL=mp4DiskBuffer.js.map