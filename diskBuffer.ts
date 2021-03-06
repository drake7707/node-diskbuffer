import * as fs from 'async-file';

import * as path from "path";
import { Stream, prototype } from 'stream';
import { read } from 'fs';
import { CancelToken } from './cancelToken';




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

async function isBufferOverflow(outputFile: string, keepMaxFiles: number): Promise<boolean> {
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
        return true;
    }

    return false;
}

async function pipeStreamToFiles(stream: NodeJS.ReadableStream, outputFile: string, maxSizeBytes: number, cancelToken: CancelToken, keepMaxFiles?: number, throwErrorOnBufferOverflow: boolean = true) {

    return new Promise<void>(async (then, reject) => {
        let curFile = await getLatestFile(outputFile, maxSizeBytes);
        let writeStream = fs.createWriteStream(curFile, {
            flags: "a"
        });
        await openStream(writeStream);

        let dataCounter = (await fs.stat(curFile)).size;

        stream.on("data", async (chunk: Buffer) => {
            stream.pause();

            if (dataCounter + chunk.length > maxSizeBytes) {

                let remainderSize = maxSizeBytes - dataCounter;
                if (remainderSize > 0) {
                    let remainder = chunk.slice(0, remainderSize);
                    let err = await writeToStream(writeStream, remainder);
                    if (err)
                        console.error("Error writing to file " + curFile + ": " + err);
                    chunk = chunk.slice(remainderSize);
                }

                // close the current file and  create a new one
                writeStream.close();

                curFile = getNextFile(outputFile, curFile);
                writeStream = fs.createWriteStream(curFile);
                await openStream(writeStream);

                if (keepMaxFiles) {
                    // check if the nr of files in the folder doesn't exceed the max limit
                    if (throwErrorOnBufferOverflow) {
                        let bufferOverflow = await isBufferOverflow(outputFile, keepMaxFiles);
                        if (bufferOverflow) {
                            reject(new Error("Buffer overflow"));;
                        }
                    }
                    else {
                        removeOldestFiles(outputFile, keepMaxFiles);
                    }
                }
                dataCounter = 0;
            }

            let err = await writeToStream(writeStream, chunk);
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
        });

        stream.on("end", () => {
            writeStream.close();
            then();
        });

        stream.on("close", () => {
            writeStream.close();
            then();
        });
    });
}


interface PipeToStreamResult {
    isFinished: boolean;
    bytesWritten: number;
}
async function pipeFileToStream(file: string, outStream: NodeJS.WritableStream, expectedChunkSize: number, offset: number, nrOfBytesToRead:number): Promise<PipeToStreamResult> {
    return new Promise<PipeToStreamResult>(async (then, reject) => {
        try {
          
            let readStream = fs.createReadStream(file, {
                flags: "r",
                start: offset,
                end:  offset + nrOfBytesToRead // must read until a specific point and not EOF or else read chunks will get corrupted and skip a bunch of bytes occassionally (probably due to race conditions of being written to at the same time)
            });

            let dataCounter = offset;

            await openStream(readStream);

            let isFinished = false;

            readStream.on("data", async (chunk:Buffer) => {
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

                await writeToStream(outStream, chunk);
              //  console.log("Resuming stream");
                readStream.resume();

            });

            readStream.on("end", () => {
                // we're done reading
                //console.log(`Done reading from file, data counter is at ${dataCounter}`)
                readStream.close();
                then({
                    bytesWritten: dataCounter,
                    isFinished: isFinished
                });
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

async function pipeFilesToStream(inputFile: string, outStream: NodeJS.WritableStream, expectedChunkSize: number, cancelToken: CancelToken) {

    let stop = false;
    outStream.on("close", () => {
        console.log("Output stream was closed");
        stop = true;
    });

    let globalBytesWrittenToOutputStream = 0;

    while (!stop && !cancelToken.isCancelled()) {
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
            while (!isFinished && !cancelToken.isCancelled()) {
                let fileSize = (await fs.stat(file)).size;

                if (dataOffset < fileSize) {

                    //console.log("Piping " + file + " with offset " + dataOffset + " to the stream");
                    let nrOfBytesToRead = fileSize - dataOffset;
                    let result = await pipeFileToStream(file, outStream, expectedChunkSize, dataOffset, nrOfBytesToRead);
                    globalBytesWrittenToOutputStream += result.bytesWritten - dataOffset;
                    console.log(`Written ${result.bytesWritten - dataOffset} bytes from ${file} with offset ${dataOffset}, total bytes sent to outputstream is ${globalBytesWrittenToOutputStream}`);
                    isFinished = result.isFinished;
                    dataOffset = result.bytesWritten;
                }

                if (!isFinished) {
                    // console.warn("Buffer underrun, waiting for data to become available");
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

    private cancelToken: CancelToken;

    constructor(private pathAndFileFormat: string, private chunkSize: number, private keepMaxFiles?: number) {
        this.cancelToken = new CancelToken();
    }

    async pipeToDisk(readableStream: NodeJS.ReadableStream) {
        await pipeStreamToFiles(readableStream, this.pathAndFileFormat, this.chunkSize, this.cancelToken, this.keepMaxFiles);
    }

    async cancel() {
        this.cancelToken.cancel();
    }
}


const deleteFolderRecursive = async (p: string) => {
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

    constructor(private pathAndFileFormat: string, private expectedChunkSize: number) {
        this.cancelToken = new CancelToken();
    }


    async pipeFromDisk(writeableStream: NodeJS.WritableStream) {
        await pipeFilesToStream(this.pathAndFileFormat, writeableStream, this.expectedChunkSize, this.cancelToken);
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
