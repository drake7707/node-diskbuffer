"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
var fs = __importStar(require("async-file"));
var path = __importStar(require("path"));
function canWriteToFile(outputFile, maxSizeBytes) {
    return __awaiter(this, void 0, void 0, function () {
        var exists, fileSize, remainingSize, expectedFileLength;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, fs.exists(outputFile)];
                case 1:
                    exists = _a.sent();
                    if (!exists)
                        return [2 /*return*/, true];
                    return [4 /*yield*/, fs.stat(outputFile)];
                case 2:
                    fileSize = (_a.sent()).size;
                    remainingSize = maxSizeBytes - fileSize;
                    if (remainingSize > 0)
                        return [2 /*return*/, true];
                    return [4 /*yield*/, getExpectedFileLengthForCorrectMP4AtomAlignment(outputFile)];
                case 3:
                    expectedFileLength = _a.sent();
                    if (expectedFileLength < fileSize || expectedFileLength > fileSize) {
                        // file needs some work (either truncate or append with padding)
                        return [2 /*return*/, true];
                    }
                    return [2 /*return*/, false];
            }
        });
    });
}
function getNextFile(outputFile, oldFile) {
    var oldFilename = path.basename(oldFile);
    var baseFilename = path.basename(outputFile);
    var folder = oldFile.substr(0, oldFile.length - oldFilename.length);
    var extension = path.extname(oldFile);
    var oldFileWithoutExtension = oldFilename.substr(0, oldFilename.length - extension.length);
    var baseFileWithoutExtension = baseFilename.substr(0, baseFilename.length - extension.length);
    var suffix = oldFileWithoutExtension.substr(baseFileWithoutExtension.length + 1);
    var nr = parseInt(suffix.substr(0, suffix.length - 1));
    nr++;
    return path.join(folder, baseFileWithoutExtension + "(" + nr + ")" + extension);
}
function getInitFilePath(outputFile) {
    var curFile;
    var filename = path.basename(outputFile);
    var folder = outputFile.substr(0, outputFile.length - filename.length);
    var extension = path.extname(outputFile);
    var fileWithoutExtension = filename.substr(0, filename.length - extension.length);
    return path.join(folder, "init" + extension);
}
function getLatestFilePath(outputFile, maxSizeBytes) {
    return __awaiter(this, void 0, void 0, function () {
        var curFile, filename, folder, extension, fileWithoutExtension, currentFiles, matchingFiles, maxCounter, i, suffix, nr, counter;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    filename = path.basename(outputFile);
                    folder = outputFile.substr(0, outputFile.length - filename.length);
                    extension = path.extname(outputFile);
                    fileWithoutExtension = filename.substr(0, filename.length - extension.length);
                    return [4 /*yield*/, fs.readdir(folder)];
                case 1:
                    currentFiles = _a.sent();
                    matchingFiles = currentFiles.filter(function (file) { return file.startsWith(fileWithoutExtension); });
                    maxCounter = 0;
                    for (i = 0; i < matchingFiles.length; i++) {
                        suffix = matchingFiles[i].substr(fileWithoutExtension.length + 1);
                        nr = suffix.substr(0, suffix.length - extension.length - 1);
                        if (parseInt(nr) > maxCounter)
                            maxCounter = parseInt(nr);
                    }
                    counter = maxCounter;
                    _a.label = 2;
                case 2:
                    // make curFile unique
                    curFile = path.join(folder, fileWithoutExtension + "(" + counter + ")" + extension);
                    counter++;
                    _a.label = 3;
                case 3: return [4 /*yield*/, canWriteToFile(curFile, maxSizeBytes)];
                case 4:
                    if (!(_a.sent())) return [3 /*break*/, 2];
                    _a.label = 5;
                case 5: return [2 /*return*/, curFile];
            }
        });
    });
}
function writeToStream(stream, chunk) {
    return new Promise(function (then, reject) {
        try {
            stream.write(chunk, function (err) {
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
    return new Promise(function (then, reject) {
        stream.once("open", function (fd) {
            then(null);
        });
        stream.once("error", function (err) {
            reject(err);
        });
    });
}
function removeOldestFiles(outputFile, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function () {
        var filename, folder, extension, fileWithoutExtension, currentFiles, matchingFilesNrs, sortedNrs, i, filepath;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    filename = path.basename(outputFile);
                    folder = outputFile.substr(0, outputFile.length - filename.length);
                    extension = path.extname(outputFile);
                    fileWithoutExtension = filename.substr(0, filename.length - extension.length);
                    return [4 /*yield*/, fs.readdir(folder)];
                case 1:
                    currentFiles = _a.sent();
                    matchingFilesNrs = currentFiles.filter(function (file) { return file.startsWith(fileWithoutExtension); })
                        .map(function (file) {
                        var suffix = file.substr(fileWithoutExtension.length + 1);
                        var nr = suffix.substr(0, suffix.length - extension.length - 1);
                        return parseInt(nr);
                    });
                    if (!(matchingFilesNrs.length > keepMaxFiles)) return [3 /*break*/, 5];
                    sortedNrs = matchingFilesNrs.sort(function (a, b) { return a - b; });
                    i = 0;
                    _a.label = 2;
                case 2:
                    if (!(i < sortedNrs.length - keepMaxFiles)) return [3 /*break*/, 5];
                    filepath = path.join(folder, fileWithoutExtension + "(" + sortedNrs[i] + ")" + extension);
                    return [4 /*yield*/, fs.delete(filepath)];
                case 3:
                    _a.sent();
                    _a.label = 4;
                case 4:
                    i++;
                    return [3 /*break*/, 2];
                case 5: return [2 /*return*/];
            }
        });
    });
}
function getExpectedFileLengthForCorrectMP4AtomAlignment(filepath) {
    return __awaiter(this, void 0, void 0, function () {
        var stream;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    stream = fs.createReadStream(filepath);
                    return [4 /*yield*/, openStream(stream)];
                case 1:
                    _a.sent();
                    return [2 /*return*/, new Promise(function (then, reject) {
                            var mp4AtomOffset = 0;
                            var totalDataLength = 0;
                            var partialAtomBuffer = null;
                            stream.on("data", function (buffer) { return __awaiter(_this, void 0, void 0, function () {
                                var relativeMP4AtomOffsetInBuffer, mp4AtomLength, relativeOffset;
                                return __generator(this, function (_a) {
                                    if (partialAtomBuffer != null) {
                                        // there was a partial atom length+ atom offset
                                        // prepend it to the buffer
                                        buffer = Buffer.concat([partialAtomBuffer, buffer]);
                                        partialAtomBuffer = null;
                                    }
                                    while (mp4AtomOffset + 4 < totalDataLength + buffer.length) {
                                        relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                                        mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                                        mp4AtomOffset += mp4AtomLength;
                                    }
                                    if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 4 >= totalDataLength + buffer.length) {
                                        relativeOffset = mp4AtomOffset - totalDataLength;
                                        partialAtomBuffer = buffer.slice(relativeOffset);
                                        buffer = buffer.slice(0, relativeOffset);
                                    }
                                    return [2 /*return*/];
                                });
                            }); });
                            stream.on("end", function () {
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
                        })];
            }
        });
    });
}
function pipeStreamToFiles(stream, outputFile, maxSizeBytes, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function () {
        var curFile, currentChunkFileSize, _a, chunkFileShouldBeOfLength, padding, writeStream, totalDataLength, mp4AtomOffset, initBuffer, initBufferNeedsToBeAppendedUntilNextMP4Atom, initBufferProcessingAtom, partialAtomBuffer;
        var _this = this;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, getLatestFilePath(outputFile, maxSizeBytes)];
                case 1:
                    curFile = _b.sent();
                    return [4 /*yield*/, fs.exists(curFile)];
                case 2:
                    if (!(!(_b.sent()))) return [3 /*break*/, 3];
                    _a = 0;
                    return [3 /*break*/, 5];
                case 3: return [4 /*yield*/, fs.stat(curFile)];
                case 4:
                    _a = (_b.sent()).size;
                    _b.label = 5;
                case 5:
                    currentChunkFileSize = _a;
                    if (!(currentChunkFileSize > 0)) return [3 /*break*/, 11];
                    return [4 /*yield*/, getExpectedFileLengthForCorrectMP4AtomAlignment(curFile)];
                case 6:
                    chunkFileShouldBeOfLength = _b.sent();
                    if (!(currentChunkFileSize < chunkFileShouldBeOfLength)) return [3 /*break*/, 8];
                    // pad with zeroes
                    console.log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, padding with zeroes");
                    padding = new Array(chunkFileShouldBeOfLength - currentChunkFileSize).fill(0);
                    return [4 /*yield*/, fs.appendFile(curFile, Buffer.from(padding))];
                case 7:
                    _b.sent();
                    return [3 /*break*/, 11];
                case 8:
                    if (!(currentChunkFileSize > chunkFileShouldBeOfLength)) return [3 /*break*/, 10];
                    // file needs to be truncated to align the proper mp4 atom
                    console.log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, truncating to last expected mp4 atom position");
                    return [4 /*yield*/, fs.truncate(curFile, chunkFileShouldBeOfLength)];
                case 9:
                    _b.sent();
                    return [3 /*break*/, 11];
                case 10:
                    // hey it perfectly aligns, nothing needs to be done
                    console.log("Continuing existing chunk " + curFile);
                    _b.label = 11;
                case 11:
                    writeStream = fs.createWriteStream(curFile, {
                        flags: "a"
                    });
                    return [4 /*yield*/, openStream(writeStream)];
                case 12:
                    _b.sent();
                    totalDataLength = 0;
                    mp4AtomOffset = 0;
                    initBuffer = Buffer.from([]);
                    initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                    initBufferProcessingAtom = "";
                    partialAtomBuffer = null;
                    stream.on("data", function (buffer) { return __awaiter(_this, void 0, void 0, function () {
                        var nextRelativeMP4AtomOffsetInBuffer, initPath, relativeMP4AtomOffsetInBuffer, mp4AtomLength, atom, initPath, relativeOffset, remainderSize, remainder, err_1, err;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    stream.pause();
                                    console.log("Data buffer received " + totalDataLength + " -> " + (totalDataLength + buffer.length));
                                    // ----- CHECK: Atom was fragmented over multiple data events?
                                    if (partialAtomBuffer != null) {
                                        // there was a partial atom length+ atom offset
                                        // prepend it to the buffer
                                        buffer = Buffer.concat([partialAtomBuffer, buffer]);
                                        partialAtomBuffer = null;
                                    }
                                    if (!initBufferNeedsToBeAppendedUntilNextMP4Atom) return [3 /*break*/, 3];
                                    nextRelativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                                    if (nextRelativeMP4AtomOffsetInBuffer < buffer.length) {
                                        initBuffer = Buffer.concat([initBuffer, buffer.slice(0, nextRelativeMP4AtomOffsetInBuffer)]);
                                        initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                                    }
                                    else {
                                        // the part is not complete, need to continue reading on the next data event
                                        initBuffer = Buffer.concat([initBuffer, buffer.slice(0, nextRelativeMP4AtomOffsetInBuffer)]);
                                        initBufferNeedsToBeAppendedUntilNextMP4Atom = true;
                                    }
                                    if (!(!initBufferNeedsToBeAppendedUntilNextMP4Atom && initBufferProcessingAtom == "moov")) return [3 /*break*/, 2];
                                    initPath = getInitFilePath(outputFile);
                                    return [4 /*yield*/, fs.writeFile(initPath, initBuffer)];
                                case 1:
                                    _a.sent();
                                    initBuffer = Buffer.from([]);
                                    _a.label = 2;
                                case 2:
                                    // clear, the atom is complete
                                    if (!initBufferNeedsToBeAppendedUntilNextMP4Atom)
                                        initBufferProcessingAtom = "";
                                    _a.label = 3;
                                case 3:
                                    relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                                    _a.label = 4;
                                case 4:
                                    if (!(mp4AtomOffset + 8 < totalDataLength + buffer.length)) return [3 /*break*/, 9];
                                    relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
                                    mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                                    atom = buffer.toString("utf8", relativeMP4AtomOffsetInBuffer + 4, relativeMP4AtomOffsetInBuffer + 4 + 4);
                                    console.log("mp4 atom '" + atom + "' offset: " + mp4AtomOffset + " -> " + (mp4AtomOffset + mp4AtomLength));
                                    if (!(atom == "ftyp")) return [3 /*break*/, 5];
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
                                    return [3 /*break*/, 8];
                                case 5:
                                    if (!(atom == "moov")) return [3 /*break*/, 8];
                                    initBufferProcessingAtom = atom;
                                    if (!(relativeMP4AtomOffsetInBuffer + mp4AtomLength < buffer.length)) return [3 /*break*/, 7];
                                    initBuffer = Buffer.concat([initBuffer, buffer.slice(relativeMP4AtomOffsetInBuffer, relativeMP4AtomOffsetInBuffer + mp4AtomLength)]);
                                    initPath = getInitFilePath(outputFile);
                                    return [4 /*yield*/, fs.writeFile(initPath, initBuffer)];
                                case 6:
                                    _a.sent();
                                    initBuffer = Buffer.from([]);
                                    initBufferNeedsToBeAppendedUntilNextMP4Atom = false;
                                    return [3 /*break*/, 8];
                                case 7:
                                    // the part is not complete, need to continue reading on the next data event
                                    initBuffer = Buffer.concat([initBuffer, buffer.slice(relativeMP4AtomOffsetInBuffer)]);
                                    initBufferNeedsToBeAppendedUntilNextMP4Atom = true;
                                    _a.label = 8;
                                case 8:
                                    mp4AtomOffset += mp4AtomLength;
                                    return [3 /*break*/, 4];
                                case 9:
                                    if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 8 >= totalDataLength + buffer.length) {
                                        relativeOffset = mp4AtomOffset - totalDataLength;
                                        partialAtomBuffer = buffer.slice(relativeOffset);
                                        buffer = buffer.slice(0, relativeOffset);
                                    }
                                    // keep track of the total data length in the entire stream
                                    totalDataLength += buffer.length;
                                    if (!(currentChunkFileSize + buffer.length > maxSizeBytes)) return [3 /*break*/, 16];
                                    if (!(relativeMP4AtomOffsetInBuffer > buffer.length)) return [3 /*break*/, 10];
                                    return [3 /*break*/, 16];
                                case 10:
                                    remainderSize = relativeMP4AtomOffsetInBuffer;
                                    if (!(remainderSize > 0)) return [3 /*break*/, 12];
                                    remainder = buffer.slice(0, remainderSize);
                                    return [4 /*yield*/, writeToStream(writeStream, remainder)];
                                case 11:
                                    err_1 = _a.sent();
                                    if (err_1)
                                        console.error("Error writing to file " + curFile + ": " + err_1);
                                    buffer = buffer.slice(remainderSize);
                                    _a.label = 12;
                                case 12:
                                    // close the current file and  create a new one
                                    writeStream.close();
                                    curFile = getNextFile(outputFile, curFile);
                                    writeStream = fs.createWriteStream(curFile);
                                    return [4 /*yield*/, openStream(writeStream)];
                                case 13:
                                    _a.sent();
                                    if (!keepMaxFiles) return [3 /*break*/, 15];
                                    // check if the nr of files in the folder doesn't exceed the max limit
                                    return [4 /*yield*/, removeOldestFiles(outputFile, keepMaxFiles)];
                                case 14:
                                    // check if the nr of files in the folder doesn't exceed the max limit
                                    _a.sent();
                                    _a.label = 15;
                                case 15:
                                    currentChunkFileSize = 0;
                                    _a.label = 16;
                                case 16: return [4 /*yield*/, writeToStream(writeStream, buffer)];
                                case 17:
                                    err = _a.sent();
                                    if (err)
                                        console.error("Error writing to file " + curFile + ": " + err);
                                    currentChunkFileSize += buffer.length;
                                    stream.resume();
                                    return [2 /*return*/];
                            }
                        });
                    }); });
                    stream.on("end", function () {
                        writeStream.close();
                    });
                    stream.on("close", function () {
                        writeStream.close();
                    });
                    return [2 /*return*/];
            }
        });
    });
}
function pipeFileToStream(file, outStream, expectedChunkSize, offset) {
    if (offset === void 0) { offset = 0; }
    return __awaiter(this, void 0, void 0, function () {
        var _this = this;
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (then, reject) { return __awaiter(_this, void 0, void 0, function () {
                    var dataCounter_1, readStream_1, mp4AtomOffset_1, isFinished_1, partialAtomBuffer_1, e_1;
                    var _this = this;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                _a.trys.push([0, 2, , 3]);
                                dataCounter_1 = offset;
                                readStream_1 = fs.createReadStream(file, {
                                    flags: "r",
                                    start: dataCounter_1
                                });
                                return [4 /*yield*/, openStream(readStream_1)];
                            case 1:
                                _a.sent();
                                mp4AtomOffset_1 = 0;
                                isFinished_1 = false;
                                partialAtomBuffer_1 = null;
                                readStream_1.on("data", function (buffer) { return __awaiter(_this, void 0, void 0, function () {
                                    var relativeMP4AtomOffsetInBuffer, mp4AtomLength, relativeOffset, realExpectedChunkSize;
                                    return __generator(this, function (_a) {
                                        switch (_a.label) {
                                            case 0:
                                                readStream_1.pause();
                                                if (partialAtomBuffer_1 != null) {
                                                    // there was a partial atom length+ atom offset
                                                    // prepend it to the buffer
                                                    buffer = Buffer.concat([partialAtomBuffer_1, buffer]);
                                                    partialAtomBuffer_1 = null;
                                                }
                                                // the disk buffer writer ensures that mp4 atoms are not fragmented over chunks
                                                // and each chunk starts with an mp4 atom 
                                                // this means that the filesize will vary anywhere between expectedChunkSize and expectedChunkSize + remainder of mp4 atom length
                                                while (mp4AtomOffset_1 + 4 < dataCounter_1 + buffer.length) {
                                                    relativeMP4AtomOffsetInBuffer = mp4AtomOffset_1 - dataCounter_1;
                                                    mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                                                    mp4AtomOffset_1 += mp4AtomLength;
                                                }
                                                if (mp4AtomOffset_1 < dataCounter_1 + buffer.length && mp4AtomOffset_1 + 4 >= dataCounter_1 + buffer.length) {
                                                    relativeOffset = mp4AtomOffset_1 - dataCounter_1;
                                                    partialAtomBuffer_1 = buffer.slice(relativeOffset);
                                                    buffer = buffer.slice(0, relativeOffset);
                                                    isFinished_1 = false;
                                                }
                                                else {
                                                    if (dataCounter_1 >= expectedChunkSize) {
                                                        realExpectedChunkSize = mp4AtomOffset_1;
                                                        if (dataCounter_1 + buffer.length >= realExpectedChunkSize)
                                                            isFinished_1 = true;
                                                    }
                                                }
                                                return [4 /*yield*/, writeToStream(outStream, buffer)];
                                            case 1:
                                                _a.sent();
                                                readStream_1.resume();
                                                dataCounter_1 += buffer.length;
                                                if (isFinished_1) {
                                                    // we're done reading
                                                    readStream_1.close();
                                                    then({
                                                        bytesWritten: dataCounter_1,
                                                        isFinished: true
                                                    });
                                                }
                                                return [2 /*return*/];
                                        }
                                    });
                                }); });
                                readStream_1.on("end", function () {
                                    if (isFinished_1) {
                                        // we're done reading
                                        readStream_1.close();
                                        then({
                                            bytesWritten: dataCounter_1,
                                            isFinished: true
                                        });
                                    }
                                    else {
                                        // still expecting more data in this file but it wasn't written yet
                                        then({
                                            bytesWritten: dataCounter_1,
                                            isFinished: false
                                        });
                                    }
                                });
                                return [3 /*break*/, 3];
                            case 2:
                                e_1 = _a.sent();
                                reject(e_1);
                                return [3 /*break*/, 3];
                            case 3: return [2 /*return*/];
                        }
                    });
                }); })];
        });
    });
}
function delay(ms) {
    return __awaiter(this, void 0, void 0, function () {
        return __generator(this, function (_a) {
            return [2 /*return*/, new Promise(function (then, reject) {
                    setTimeout(then, ms);
                })];
        });
    });
}
function pipeFilesToStream(inputFile, outStream, expectedChunkSize, writeInitChunkIfAvailable) {
    return __awaiter(this, void 0, void 0, function () {
        var initFile_1, stop, _loop_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (!writeInitChunkIfAvailable) return [3 /*break*/, 3];
                    initFile_1 = getInitFilePath(inputFile);
                    return [4 /*yield*/, fs.exists(initFile_1)];
                case 1:
                    if (!_a.sent()) return [3 /*break*/, 3];
                    return [4 /*yield*/, new Promise(function (then, reject) {
                            var initStream = fs.createReadStream(initFile_1);
                            initStream.on("data", function (buffer) {
                                outStream.write(buffer);
                            });
                            initStream.on("end", function () {
                                initStream.close();
                                then();
                            });
                        })];
                case 2:
                    _a.sent();
                    _a.label = 3;
                case 3:
                    stop = false;
                    outStream.on("close", function () {
                        console.log("Output stream was closed");
                        stop = true;
                    });
                    _loop_1 = function () {
                        var files, filename, folder, extension, fileWithoutExtension, currentFiles, matchingFilesNrs, sortedNrs, i, filepath, file, isFinished, dataOffset, fileSize, result, result;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    files = [];
                                    filename = path.basename(inputFile);
                                    folder = inputFile.substr(0, inputFile.length - filename.length);
                                    extension = path.extname(inputFile);
                                    fileWithoutExtension = filename.substr(0, filename.length - extension.length);
                                    return [4 /*yield*/, fs.readdir(folder)];
                                case 1:
                                    currentFiles = _a.sent();
                                    matchingFilesNrs = currentFiles.filter(function (file) { return file.startsWith(fileWithoutExtension); })
                                        .map(function (file) {
                                        var suffix = file.substr(fileWithoutExtension.length + 1);
                                        var nr = suffix.substr(0, suffix.length - extension.length - 1);
                                        return parseInt(nr);
                                    });
                                    sortedNrs = matchingFilesNrs.sort(function (a, b) { return a - b; });
                                    for (i = 0; i < sortedNrs.length; i++) {
                                        filepath = path.join(folder, fileWithoutExtension + "(" + sortedNrs[i] + ")" + extension);
                                        files.push(filepath);
                                    }
                                    _a.label = 2;
                                case 2:
                                    if (!(files.length > 0)) return [3 /*break*/, 13];
                                    file = files.shift();
                                    isFinished = false;
                                    dataOffset = 0;
                                    _a.label = 3;
                                case 3:
                                    if (!!isFinished) return [3 /*break*/, 11];
                                    return [4 /*yield*/, fs.stat(file)];
                                case 4:
                                    fileSize = (_a.sent()).size;
                                    if (!(files.length > 1)) return [3 /*break*/, 6];
                                    // it's easy if it's not the last file, just pipe the whole file to the output
                                    if (fileSize < expectedChunkSize) {
                                        // this file is not the latest one but it's not the expected chunk size, corruption will occur
                                        console.warn("Filesize of chunk " + file + " is unexpectedly less (" + fileSize + ") than the expected chunk size (" + expectedChunkSize + "). Corruption is highly probable");
                                    }
                                    console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                                    return [4 /*yield*/, pipeFileToStream(file, outStream, expectedChunkSize, dataOffset)];
                                case 5:
                                    result = _a.sent();
                                    isFinished = result.isFinished;
                                    dataOffset = result.bytesWritten;
                                    _a.label = 6;
                                case 6:
                                    if (!(dataOffset < fileSize)) return [3 /*break*/, 8];
                                    console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                                    return [4 /*yield*/, pipeFileToStream(file, outStream, expectedChunkSize, dataOffset)];
                                case 7:
                                    result = _a.sent();
                                    isFinished = result.isFinished;
                                    dataOffset = result.bytesWritten;
                                    _a.label = 8;
                                case 8:
                                    if (!!isFinished) return [3 /*break*/, 10];
                                    console.warn("Buffer underrun, waiting for data to become available");
                                    return [4 /*yield*/, delay(100)];
                                case 9:
                                    _a.sent(); //  wait a bit for data to become available
                                    _a.label = 10;
                                case 10: return [3 /*break*/, 3];
                                case 11: 
                                // now clean it up
                                return [4 /*yield*/, fs.delete(file)];
                                case 12:
                                    // now clean it up
                                    _a.sent();
                                    return [3 /*break*/, 2];
                                case 13: 
                                // prevent hammering with tight loop
                                return [4 /*yield*/, delay(100)];
                                case 14:
                                    // prevent hammering with tight loop
                                    _a.sent();
                                    return [2 /*return*/];
                            }
                        });
                    };
                    _a.label = 4;
                case 4:
                    if (!!stop) return [3 /*break*/, 6];
                    return [5 /*yield**/, _loop_1()];
                case 5:
                    _a.sent();
                    return [3 /*break*/, 4];
                case 6: return [2 /*return*/];
            }
        });
    });
}
var DiskBufferWriter = /** @class */ (function () {
    function DiskBufferWriter(pathAndFileFormat, chunkSize, keepMaxFiles) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.chunkSize = chunkSize;
        this.keepMaxFiles = keepMaxFiles;
    }
    DiskBufferWriter.prototype.pipeToDisk = function (readableStream) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.keepMaxFiles)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    return DiskBufferWriter;
}());
exports.DiskBufferWriter = DiskBufferWriter;
var DiskBufferReader = /** @class */ (function () {
    function DiskBufferReader(pathAndFileFormat, expectedChunkSize, writeInitChunkIfAvailable) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.expectedChunkSize = expectedChunkSize;
        this.writeInitChunkIfAvailable = writeInitChunkIfAvailable;
    }
    DiskBufferReader.prototype.pipeFromDisk = function (writeableStream) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize, this.writeInitChunkIfAvailable)];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    return DiskBufferReader;
}());
exports.DiskBufferReader = DiskBufferReader;
//# sourceMappingURL=mp4DiskBuffer.js.map