import * as fs from 'async-file';

import * as path from "path";

export var DEBUG: boolean = false;

function log(msg: string) {
    if (DEBUG)
        console.log(msg);
}
function error(msg: string) {
    if (DEBUG)
        console.error(msg);
}

async function canWriteToFile(outputFile: string, maxSizeBytes: number) {
    let exists = await fs.exists(outputFile);
    if (!exists)
        return true;

    let fileSize = (await fs.stat(outputFile)).size;
    let remainingSize = maxSizeBytes - fileSize;
    if (remainingSize > 0)
        return true;

    // it's possible that the mp4 atom needs to be finished so
    // remaining size depends on maxSizeBytes + length of mp4offset

    let expectedFileLength = await getExpectedFileLengthForCorrectMP4AtomAlignment(outputFile);
    if (expectedFileLength < fileSize || expectedFileLength > fileSize) {
        // file needs some work (either truncate or append with padding)
        return true;
    }

    return false;
}

function getNextFile(outputFile: string, oldFile: string): string {
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

function getInitFilePath(outputFile: string): string {
    let curFile: string;

    let filename = path.basename(outputFile);
    let folder = outputFile.substr(0, outputFile.length - filename.length);
    let extension = path.extname(outputFile);
    let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
    return path.join(folder, "init" + extension);
}

async function getLatestFilePath(outputFile: string, maxSizeBytes: number): Promise<string> {
    let curFile: string;

    let filename = path.basename(outputFile);
    let folder = outputFile.substr(0, outputFile.length - filename.length);
    let extension = path.extname(outputFile);
    let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
    let currentFiles = await fs.readdir(folder);
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
    } while (!await canWriteToFile(curFile, maxSizeBytes))
    return curFile;
}


function writeToStream(stream: NodeJS.WritableStream, chunk: any) {
    return new Promise<any>((then, reject) => {
        try {
            stream.write(chunk, (err: any) => {
                if (err)
                    then(err);
                else
                    then(null);
            });
        } catch (e) {
            reject(e);
        }
    });
}


function openStream(stream: fs.WriteStream | fs.ReadStream) {
    return new Promise<any>((then, reject) => {
        stream.once("open", fd => {
            then(null);
        });
        stream.once("error", err => {
            reject(err);
        });
    });
}

async function removeOldestFiles(outputFile: string, keepMaxFiles: number) {
    let filename = path.basename(outputFile);
    let folder = outputFile.substr(0, outputFile.length - filename.length);
    let extension = path.extname(outputFile);
    let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
    let currentFiles = await fs.readdir(folder);
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
            await fs.delete(filepath);
        }
    }
}

async function getExpectedFileLengthForCorrectMP4AtomAlignment(filepath: string): Promise<number> {

    let stream = fs.createReadStream(filepath);
    await openStream(stream);

    let result = await new Promise<number>((then, reject) => {
        let mp4AtomOffset = 0;
        let totalDataLength = 0;
        let partialAtomBuffer: Buffer | null = null;

        stream.on("data", async (buffer: Buffer) => {

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
        });

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
}



async function pipeStreamToFiles(stream: NodeJS.ReadableStream, outputFile: string, maxSizeBytes: number, keepMaxFiles?: number) {

    let curFile = await getLatestFilePath(outputFile, maxSizeBytes);
    let currentChunkFileSize = (!await fs.exists(curFile)) ? 0 : (await fs.stat(curFile)).size;

    if (currentChunkFileSize > 0) {
        // there was already an existing chunk file that needs to be continued
        // the file might not be aligned on a correct mp4 atom 
        let chunkFileShouldBeOfLength = await getExpectedFileLengthForCorrectMP4AtomAlignment(curFile);

        if (currentChunkFileSize < chunkFileShouldBeOfLength) {
            // pad with zeroes
            log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, padding with zeroes");
            let padding = new Array(chunkFileShouldBeOfLength - currentChunkFileSize).fill(0);
            await fs.appendFile(curFile, Buffer.from(padding));
        }
        else if (currentChunkFileSize > chunkFileShouldBeOfLength) {
            // file needs to be truncated to align the proper mp4 atom
            log("Continuing existing chunk " + curFile + " but file is not properly aligned to mp4 atoms, truncating to last expected mp4 atom position");
            await fs.truncate(curFile, chunkFileShouldBeOfLength);
        }
        else {
            // hey it perfectly aligns, nothing needs to be done
            log("Continuing existing chunk " + curFile);
        }
    }


    let writeStream = fs.createWriteStream(curFile, {
        flags: "a"
    });
    await openStream(writeStream);




    let totalDataLength = 0;
    let mp4AtomOffset = 0;

    // track the offset of the last moof. Chunks should not only be mp4 atom aligned, they should only split on moof
    // because [ftyp+moov]+mdat is invalid
    let moofMP4AtomOffset = -1;


    let initBuffer: Buffer = Buffer.from([]);
    let initBufferNeedsToBeAppendedUntilNextMP4Atom: boolean = false;
    let initBufferProcessingAtom: string = "";

    let partialAtomBuffer: Buffer | null = null;

    stream.on("data", async (buffer: Buffer) => {
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
                await fs.writeFile(initPath, initBuffer);
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
            } else if (atom == "moov") {
                initBufferProcessingAtom = atom;

                if (relativeMP4AtomOffsetInBuffer + mp4AtomLength < buffer.length) {
                    initBuffer = Buffer.concat([initBuffer, buffer.slice(relativeMP4AtomOffsetInBuffer, relativeMP4AtomOffsetInBuffer + mp4AtomLength)]);
                    // finished the moov atom, write the init file to a separate file
                    let initPath = getInitFilePath(outputFile);
                    await fs.writeFile(initPath, initBuffer);
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

            if(relativeMOOFMP4AtomOffsetInBuffer < 0) {
                // wait until we have a moof atom in the buffer before splitting
            } else if(relativeMOOFMP4AtomOffsetInBuffer >= buffer.length) {
                // this can't happen because the MOOF should be encountered in the buffer
            }
            else {
                // split on MOOF atom. This ensures that a chunk will always be
                // ftyp+moov+moof+mdat, which is required for ffmpeg.

                let remainderSize = relativeMOOFMP4AtomOffsetInBuffer;
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




        let err = await writeToStream(writeStream, buffer);
        if (err)
            error("Error writing to file " + curFile + ": " + err);
        currentChunkFileSize += buffer.length;

        stream.resume();

    });
    stream.on("end", () => {
        writeStream.close();
    });

    stream.on("close", () => {
        writeStream.close();
    });
}

interface PipeToStreamResult {
    isFinished: boolean;
    readUntilOffset: number;
    nextMP4AtomOffset: number;
}
async function pipeFileToStream(file: string, outStream: NodeJS.WritableStream, expectedChunkSize: number, offset: number = 0, startMP4AtomOffset: number = 0): Promise<PipeToStreamResult> {
    return new Promise<PipeToStreamResult>(async (then, reject) => {
        try {
            let curPositionInFile = offset;

            let readStream = fs.createReadStream(file, {
                flags: "r",
                start: curPositionInFile
            });
            await openStream(readStream);

            let mp4AtomOffset = startMP4AtomOffset;

            let isFinished = false;
            let partialAtomBuffer: Buffer | null = null;

            readStream.on("data", async buffer => {
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

                await writeToStream(outStream, buffer);

                // resume to get the next data event
                readStream.resume();
            });

            readStream.on("end", () => {
                if (isFinished) {
                    // we're done reading
                    readStream.close();
                    then({
                        readUntilOffset: curPositionInFile,
                        nextMP4AtomOffset: mp4AtomOffset,
                        isFinished: true
                    });
                } else {
                    // still expecting more data in this file but it wasn't written yet
                    then({
                        readUntilOffset: curPositionInFile,
                        nextMP4AtomOffset: mp4AtomOffset,
                        isFinished: false
                    });
                }
            });


        } catch (e) {
            reject(e);
        }
    });
}

async function delay(ms: number) {
    return new Promise<void>((then, reject) => {
        setTimeout(then, ms);
    });
}



interface CheckFileForInitResult {
    hasInit: boolean,
    offsetAfterInit: number,
    notEnoughData: boolean,
}

async function fileStartsWithFtypAndMoov(inputFile: string): Promise<CheckFileForInitResult> {


    let stream = fs.createReadStream(inputFile);
    await openStream(stream);

    return new Promise<CheckFileForInitResult>((then, reject) => {
        let mp4AtomOffset = 0;
        let totalDataLength = 0;
        let partialAtomBuffer: Buffer | null = null;

        let atoms: string[] = [];

        stream.on("data", async (buffer: Buffer) => {

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
        });

        stream.on("end", () => {
            if (atoms.length == 2) {
                if (atoms[0] == "ftyp" && atoms[1] == "moov")
                    then({ hasInit: true, offsetAfterInit: mp4AtomOffset, notEnoughData: false });
                else
                    then({ hasInit: false, offsetAfterInit: -1, notEnoughData: false });

                stream.close();
            } else if (atoms.length < 2) {
                // not enough data to read yet
                then({ hasInit: false, offsetAfterInit: -1, notEnoughData: true })
            } else {
                // read more than 2 atoms, but they don't start with ftyp and moov
                then({ hasInit: false, offsetAfterInit: -1, notEnoughData: false });
            }

        });
    });
}


class CancelToken {
    private cancelled: boolean = false;
    isCancelled() {
        return this.cancelled;
    }

    cancel() {
        this.cancelled = true;
    }
}

async function pipeFilesToStream(inputFile: string, outStream: NodeJS.WritableStream, expectedChunkSize: number, writeInitChunkIfAvailable: boolean, cancelToken: CancelToken) {

    if (writeInitChunkIfAvailable) {
        let initFile = getInitFilePath(inputFile);

        log("Waiting for init file " + initFile + " to become available ");
        while (!await fs.exists(initFile) && !cancelToken.isCancelled()) {
            await delay(100);
        }

        if (cancelToken.isCancelled())
            return;

        if (await fs.exists(initFile)) {
            await new Promise<void>((then, reject) => {
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
        let files: string[] = [];
        let filename = path.basename(inputFile);
        let folder = inputFile.substr(0, inputFile.length - filename.length);
        let extension = path.extname(inputFile);
        let fileWithoutExtension = filename.substr(0, filename.length - extension.length);
        let currentFiles = await fs.readdir(folder);
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
            let file = files.shift()!;
            log(`Processing file ${file}`);
            await processFileToPipeToStream(file, writeInitChunkIfAvailable, cancelToken, outStream, expectedChunkSize);

        }

        // prevent hammering with tight loop
        await delay(100);
    }
}

async function processFileToPipeToStream(file: string, writeInitChunkIfAvailable: boolean, cancelToken: CancelToken, outStream: NodeJS.WritableStream, expectedChunkSize: number) {
    let fileStillExists = await fs.exists(file);
    if (!fileStillExists) {
        log(`File ${file} doesn't exist anymore, skipping file`)
        return;
    }

    let isFinished = false;
    let dataOffset = 0;
    let nextMP4AtomOffset = 0;

    if (dataOffset == 0 && writeInitChunkIfAvailable) {
        // must check if the file doesn't start with an init chunk

        let result: CheckFileForInitResult | null = null;
        let hasEnoughData = false;
        while (!hasEnoughData && !cancelToken.isCancelled()) {

            fileStillExists = await fs.exists(file);
            if (!fileStillExists) {
                log(`File ${file} doesn't exist anymore, aborting check to see if the file starts with init atoms`)
                return;
            }

            result = await fileStartsWithFtypAndMoov(file);
            hasEnoughData = !result.notEnoughData;
            if (!hasEnoughData)
                await delay(100);
        }

        if (cancelToken.isCancelled()) {
            log("Reader was cancelled while waiting for init chunk to become available")
        }

        if (result != null && result.hasInit) {
            log(`File ${file} has init segment, the init chunk was already injected in the stream so skipping until after the init part to offset ${result.offsetAfterInit}`)
            dataOffset = result.offsetAfterInit;
            nextMP4AtomOffset = result.offsetAfterInit;
        } else {
            log(`File does not have an init segment, can just read from the beginning of the file`);
        }
    }


    while (!isFinished && !cancelToken.isCancelled()) {
        let fileSize = (await fs.stat(file)).size;

        fileStillExists = await fs.exists(file);
        if (!fileStillExists) {
            log(`File ${file} doesn't exist anymore, aborting pipe`)
            return;
        }

        if (dataOffset < fileSize) {
            log("Piping " + file + " with offset " + dataOffset + " to the stream");
            let result = await pipeFileToStream(file, outStream, expectedChunkSize, dataOffset, nextMP4AtomOffset);
            log(`Data of ${file} was piped to the stream until offset ${result.readUntilOffset}, isFinished=${result.isFinished}, next mp4 offset is at ${result.nextMP4AtomOffset}`);
            isFinished = result.isFinished;
            dataOffset = result.readUntilOffset;
            nextMP4AtomOffset = result.nextMP4AtomOffset;
        }

        if (!isFinished) {
            //console.warn("Buffer underrun, waiting for data to become available");
            await delay(100); //  wait a bit for data to become available
        }
    }

    // now clean it up
    log(`Cleaning up file ${file}`);
    await fs.delete(file);
}





export class DiskBufferWriter {

    constructor(private pathAndFileFormat: string, private chunkSize: number, private keepMaxFiles?: number) {

    }

    async pipeToDisk(readableStream: NodeJS.ReadableStream) {
        await pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.keepMaxFiles);
    }
}


const deleteFolderRecursive = async (p:string) =>  {
    if (await fs.exists(p)) {
        for (let entry of await fs.readdir(p)) {
            const curPath = path + "/" + entry;
            if ((await fs.lstat(curPath)).isDirectory())
                await deleteFolderRecursive(curPath);
            else await fs.unlink(curPath);
        }
        await fs.rmdir(p);
    }
};

export class DiskBufferReader {

    private cancelToken: CancelToken;

    constructor(private pathAndFileFormat: string, private expectedChunkSize: number, private writeInitChunkIfAvailable: boolean) {
        this.cancelToken = new CancelToken();
    }


    async pipeFromDisk(writeableStream: NodeJS.WritableStream) {

        await pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize, this.writeInitChunkIfAvailable, this.cancelToken);
    }


    
    async cleanUp() {
        let filename = path.basename(this.pathAndFileFormat);
        let folder = this.pathAndFileFormat.substr(0, this.pathAndFileFormat.length - filename.length);
    
        console.log("Cleaning up disk buffer, deleting folder " + folder);
        await deleteFolderRecursive(folder);
    }
    
    async closeWhenBufferIsDone() {
        this.cancelToken.cancel();
    }

}
