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
        var exists, remainingSize, _a;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0: return [4 /*yield*/, fs.exists(outputFile)];
                case 1:
                    exists = _b.sent();
                    if (!exists)
                        return [2 /*return*/, true];
                    _a = maxSizeBytes;
                    return [4 /*yield*/, fs.stat(outputFile)];
                case 2:
                    remainingSize = _a - (_b.sent()).size;
                    if (remainingSize > 0)
                        return [2 /*return*/, true];
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
function getLatestFile(outputFile, maxSizeBytes) {
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
function pipeStreamToFiles(stream, outputFile, maxSizeBytes, keepMaxFiles) {
    return __awaiter(this, void 0, void 0, function () {
        var curFile, writeStream, dataCounter;
        var _this = this;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, getLatestFile(outputFile, maxSizeBytes)];
                case 1:
                    curFile = _a.sent();
                    writeStream = fs.createWriteStream(curFile, {
                        flags: "w"
                    });
                    return [4 /*yield*/, openStream(writeStream)];
                case 2:
                    _a.sent();
                    return [4 /*yield*/, fs.stat(curFile)];
                case 3:
                    dataCounter = (_a.sent()).size;
                    stream.on("data", function (chunk) { return __awaiter(_this, void 0, void 0, function () {
                        var remainderSize, remainder, err_1, err;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    stream.pause();
                                    if (!(dataCounter + chunk.length > maxSizeBytes)) return [3 /*break*/, 6];
                                    remainderSize = maxSizeBytes - dataCounter;
                                    if (!(remainderSize > 0)) return [3 /*break*/, 2];
                                    remainder = chunk.slice(0, remainderSize);
                                    return [4 /*yield*/, writeToStream(writeStream, remainder)];
                                case 1:
                                    err_1 = _a.sent();
                                    if (err_1)
                                        console.error("Error writing to file " + curFile + ": " + err_1);
                                    chunk = chunk.slice(remainderSize);
                                    _a.label = 2;
                                case 2:
                                    // close the current file and  create a new one
                                    writeStream.close();
                                    curFile = getNextFile(outputFile, curFile);
                                    writeStream = fs.createWriteStream(curFile);
                                    return [4 /*yield*/, openStream(writeStream)];
                                case 3:
                                    _a.sent();
                                    if (!keepMaxFiles) return [3 /*break*/, 5];
                                    // check if the nr of files in the folder doesn't exceed the max limit
                                    return [4 /*yield*/, removeOldestFiles(outputFile, keepMaxFiles)];
                                case 4:
                                    // check if the nr of files in the folder doesn't exceed the max limit
                                    _a.sent();
                                    _a.label = 5;
                                case 5:
                                    dataCounter = 0;
                                    _a.label = 6;
                                case 6: return [4 /*yield*/, writeToStream(writeStream, chunk)];
                                case 7:
                                    err = _a.sent();
                                    if (err)
                                        console.error("Error writing to file " + curFile + ": " + err);
                                    dataCounter += chunk.length;
                                    stream.resume();
                                    return [2 /*return*/];
                            }
                        });
                    }); });
                    stream.on("end", function () {
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
                    var dataCounter_1, readStream_1, e_1;
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
                                readStream_1.on("data", function (chunk) { return __awaiter(_this, void 0, void 0, function () {
                                    return __generator(this, function (_a) {
                                        switch (_a.label) {
                                            case 0:
                                                readStream_1.pause();
                                                return [4 /*yield*/, writeToStream(outStream, chunk)];
                                            case 1:
                                                _a.sent();
                                                readStream_1.resume();
                                                dataCounter_1 += chunk.length;
                                                if (dataCounter_1 >= expectedChunkSize) {
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
                                    if (dataCounter_1 >= expectedChunkSize) {
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
function pipeFilesToStream(inputFile, outStream, expectedChunkSize) {
    return __awaiter(this, void 0, void 0, function () {
        var _loop_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    _loop_1 = function () {
                        var files, filename, folder, extension, fileWithoutExtension, currentFiles, matchingFilesNrs, sortedNrs, i, filepath, file, isFinished, dataOffset, fileSize, result, result, e_2;
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
                                    if (!(files.length > 0)) return [3 /*break*/, 16];
                                    file = files.shift();
                                    _a.label = 3;
                                case 3:
                                    _a.trys.push([3, 13, , 14]);
                                    isFinished = false;
                                    dataOffset = 0;
                                    _a.label = 4;
                                case 4:
                                    if (!!isFinished) return [3 /*break*/, 12];
                                    return [4 /*yield*/, fs.stat(file)];
                                case 5:
                                    fileSize = (_a.sent()).size;
                                    if (!(files.length > 1)) return [3 /*break*/, 7];
                                    // it's easy if it's not the last file, just pipe the whole file to the output
                                    if (fileSize < expectedChunkSize) {
                                        // this file is not the latest one but it's not the expected chunk size, corruption will occur
                                        console.warn("Filesize of chunk " + file + " is unexpectedly less (" + fileSize + ") than the expected chunk size (" + expectedChunkSize + "). Corruption is highly probable");
                                    }
                                    return [4 /*yield*/, pipeFileToStream(file, outStream, expectedChunkSize, dataOffset)];
                                case 6:
                                    result = _a.sent();
                                    isFinished = result.isFinished;
                                    dataOffset = result.bytesWritten;
                                    _a.label = 7;
                                case 7:
                                    if (!(dataOffset < fileSize)) return [3 /*break*/, 9];
                                    return [4 /*yield*/, pipeFileToStream(file, outStream, expectedChunkSize, dataOffset)];
                                case 8:
                                    result = _a.sent();
                                    isFinished = result.isFinished;
                                    dataOffset = result.bytesWritten;
                                    _a.label = 9;
                                case 9:
                                    if (!!isFinished) return [3 /*break*/, 11];
                                    return [4 /*yield*/, delay(100)];
                                case 10:
                                    _a.sent(); //  wait a bit for data to become available
                                    _a.label = 11;
                                case 11: return [3 /*break*/, 4];
                                case 12: return [3 /*break*/, 14];
                                case 13:
                                    e_2 = _a.sent();
                                    console.error(e_2);
                                    return [3 /*break*/, 14];
                                case 14: 
                                // now clean it up
                                return [4 /*yield*/, fs.delete(file)];
                                case 15:
                                    // now clean it up
                                    _a.sent();
                                    return [3 /*break*/, 2];
                                case 16: return [2 /*return*/];
                            }
                        });
                    };
                    _a.label = 1;
                case 1:
                    if (!true) return [3 /*break*/, 3];
                    return [5 /*yield**/, _loop_1()];
                case 2:
                    _a.sent();
                    return [3 /*break*/, 1];
                case 3: return [2 /*return*/];
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
    function DiskBufferReader(pathAndFileFormat, expectedChunkSize) {
        this.pathAndFileFormat = pathAndFileFormat;
        this.expectedChunkSize = expectedChunkSize;
    }
    DiskBufferReader.prototype.pipeFromDisk = function (writeableStream) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize)];
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
//# sourceMappingURL=diskBuffer.js.map