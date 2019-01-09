import * as fs from 'async-file';

import * as path from "path";
import { Stream, prototype } from 'stream';
import { read } from 'fs';




async function canWriteToFile(outputFile: string, maxSizeBytes: number) {
    let exists = await fs.exists(outputFile);
    if (!exists)
        return true;

    let remainingSize = maxSizeBytes - (await fs.stat(outputFile)).size;
    if (remainingSize > 0)
        return true;

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

async function getLatestFile(outputFile: string, maxSizeBytes: number): Promise<string> {
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

async function pipeStreamToFiles(stream: NodeJS.ReadableStream, outputFile: string, maxSizeBytes: number, keepMaxFiles?: number) {

    let curFile = await getLatestFile(outputFile, maxSizeBytes);
    let writeStream = fs.createWriteStream(curFile, {
        flags: "a"
    });
    await openStream(writeStream);

    let dataCounter = (await fs.stat(curFile)).size;

    let totalDataLength = 0;
    let mp4AtomOffset = 0;

    let partialAtomBuffer: Buffer | null = null;

    stream.on("data", async (buffer: Buffer) => {
        stream.pause();

        if (partialAtomBuffer != null) {
            // there was a partial atom length+ atom offset
            // prepend it to the buffer
            buffer = Buffer.concat([partialAtomBuffer, buffer]);
            partialAtomBuffer = null;
        }

        console.log("Data buffer received " + totalDataLength + " -> " + (totalDataLength + buffer.length));


        let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;

        // make sure all atom length + atom is actually within the buffer (so do +8)
        while (mp4AtomOffset + 8 < totalDataLength + buffer.length) {
            relativeMP4AtomOffsetInBuffer = mp4AtomOffset - totalDataLength;
            let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);

            let atom = buffer.toString("utf8", relativeMP4AtomOffsetInBuffer + 4, relativeMP4AtomOffsetInBuffer + 4 + 4);
            console.log("mp4 atom '" + atom + "' offset: " + mp4AtomOffset + " -> " + (mp4AtomOffset + mp4AtomLength));

            mp4AtomOffset += mp4AtomLength;
        }

        if (mp4AtomOffset < totalDataLength + buffer.length && mp4AtomOffset + 8 >= totalDataLength + buffer.length) {
            // the atom length/atom is fragmented over this buffer and the next one
            // truncate the buffer to remove the partial atom, but store it in a variable
            // first, so it can be prepended for the next buffer
            let relativeOffset = mp4AtomOffset - totalDataLength;
            partialAtomBuffer = buffer.slice(relativeOffset);
            buffer = buffer.slice(0, relativeOffset);
        }

        totalDataLength += buffer.length;


        if (dataCounter + buffer.length > maxSizeBytes) {

            // relativeMP4AtomOffsetInBuffer is now the last mp4 offset in the buffer
            if (relativeMP4AtomOffsetInBuffer > buffer.length) {
                // it's beyond the buffer length, wait for the next data buffer before splitting
            }
            else {
                // rather than take the remainder size on max size bytes, take the remainder on the last mp4 atom offset
                //let remainderSize = maxSizeBytes - dataCounter;
                let remainderSize = relativeMP4AtomOffsetInBuffer;
                if (remainderSize > 0) {
                    let remainder = buffer.slice(0, remainderSize);
                    let err = await writeToStream(writeStream, remainder);
                    if (err)
                        console.error("Error writing to file " + curFile + ": " + err);
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
                dataCounter = 0;
            }
        }

        let err = await writeToStream(writeStream, buffer);
        if (err)
            console.error("Error writing to file " + curFile + ": " + err);
        dataCounter += buffer.length;

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
    bytesWritten: number;
}
async function pipeFileToStream(file: string, outStream: NodeJS.WritableStream, expectedChunkSize: number, offset: number = 0): Promise<PipeToStreamResult> {
    return new Promise<PipeToStreamResult>(async (then, reject) => {
        try {
            let dataCounter = offset;

            let readStream = fs.createReadStream(file, {
                flags: "r",
                start: dataCounter
            });
            await openStream(readStream);

            let mp4AtomOffset = 0;

            let isFinished = false;
            let partialAtomBuffer: Buffer | null = null;

            readStream.on("data", async buffer => {
                readStream.pause();

                if (partialAtomBuffer != null) {
                    // there was a partial atom length+ atom offset
                    // prepend it to the buffer
                    buffer = Buffer.concat([partialAtomBuffer, buffer]);
                    partialAtomBuffer = null;
                }

                // the disk buffer writer ensures that mp4 atoms are not fragmented over chunks
                // and each chunk starts with an mp4 atom 
                // this means that the filesize will vary anywhere between expectedChunkSize and expectedChunkSize + remainder of mp4 atom length
                while (mp4AtomOffset + 4 < dataCounter + buffer.length) {
                    let relativeMP4AtomOffsetInBuffer = mp4AtomOffset - dataCounter;
                    let mp4AtomLength = buffer.readUIntBE(relativeMP4AtomOffsetInBuffer, 4);
                    mp4AtomOffset += mp4AtomLength;
                }

                if (mp4AtomOffset < dataCounter + buffer.length && mp4AtomOffset + 4 >= dataCounter + buffer.length) {
                    // again a partial scenario, there WILL be a next buffer containing the remainder of the mp4 atom offset
                    let relativeOffset = mp4AtomOffset - dataCounter;
                    partialAtomBuffer = buffer.slice(relativeOffset);
                    buffer = buffer.slice(0, relativeOffset);

                    isFinished = false;
                }
                else {
                    if (dataCounter >= expectedChunkSize) {
                        // we've reached the expected chunk size, check if the latest mp4 atom is beyond this point
                        // the next mp4 atom is at mp4AtomOffset
                        let realExpectedChunkSize = mp4AtomOffset;
                        if (dataCounter + buffer.length >= realExpectedChunkSize)
                            isFinished = true;
                    }
                }


                await writeToStream(outStream, buffer);
                readStream.resume();
                dataCounter += buffer.length;


                if (isFinished) {
                    // we're done reading
                    readStream.close();
                    then({
                        bytesWritten: dataCounter,
                        isFinished: true
                    });
                }
            });
            readStream.on("end", () => {
                if (isFinished) {
                    // we're done reading
                    readStream.close();
                    then({
                        bytesWritten: dataCounter,
                        isFinished: true
                    });
                } else {
                    // still expecting more data in this file but it wasn't written yet
                    then({
                        bytesWritten: dataCounter,
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

async function pipeFilesToStream(inputFile: string, outStream: NodeJS.WritableStream, expectedChunkSize: number) {

    let stop = false;
    outStream.on("close", () => {
        console.log("Output stream was closed");
        stop = true;
    });

    while (!stop) {
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

        while (files.length > 0) {
            let file = files.shift()!;

            let isFinished = false;
            let dataOffset = 0;

            while (!isFinished) {
                let fileSize = (await fs.stat(file)).size;

                if (files.length > 1) {
                    // it's easy if it's not the last file, just pipe the whole file to the output

                    if (fileSize < expectedChunkSize) {
                        // this file is not the latest one but it's not the expected chunk size, corruption will occur
                        console.warn("Filesize of chunk " + file + " is unexpectedly less (" + fileSize + ") than the expected chunk size (" + expectedChunkSize + "). Corruption is highly probable");
                    }

                    console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                    let result = await pipeFileToStream(file, outStream, expectedChunkSize, dataOffset);
                    isFinished = result.isFinished;
                    dataOffset = result.bytesWritten;

                }

                if (dataOffset < fileSize) {

                    console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                    let result = await pipeFileToStream(file, outStream, expectedChunkSize, dataOffset);
                    isFinished = result.isFinished;
                    dataOffset = result.bytesWritten;
                }
                if (!isFinished) {
                    console.warn("Buffer underrun, waiting for data to become available");
                    await delay(100); //  wait a bit for data to become available
                }
            }

            // now clean it up
            await fs.delete(file);
        }

        // prevent hammering with tight loop
        await delay(100);
    }

}





export class DiskBufferWriter {

    constructor(private pathAndFileFormat: string, private chunkSize: number, private keepMaxFiles?: number) {

    }

    async pipeToDisk(readableStream: NodeJS.ReadableStream) {
        await pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.keepMaxFiles);
    }
}

export class DiskBufferReader {

    constructor(private pathAndFileFormat: string, private expectedChunkSize: number) {

    }

    async pipeFromDisk(writeableStream: NodeJS.WritableStream) {

        await pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize);
    }

}
