import AWS, {S3} from 'aws-sdk';
import * as path from 'path';
import {Directory} from '../helpers/directory';
import Path from 'path';
import * as util from 'util';
import {Files} from '../helpers/file';
import {ProgressBar} from '../helpers/progress-bar';
import {ManagedUpload} from 'aws-sdk/clients/s3';

const prompt = require('prompt');
const logger = require('../helpers/logger');
const fs = require('fs').promises;

export class RepairAvro {

    private readonly configuration: any;
    private readonly baseAvroToolsCommandLine: string;

    constructor(configuration: any) {
        this.configuration = configuration;
        this.baseAvroToolsCommandLine = `${this.configuration.environment.java} -jar -Dlog4j.configuration=file:./bin/log4j.properties ./bin/avro-tools-1.8.2.jar`;

        AWS.config.update({
            accessKeyId: process.env?.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env?.AWS_SECRET_ACCESS_KEY,
            region: process.env?.AWS_REGION,
            sessionToken: process.env?.AWS_SESSION_TOKEN
        });
    }

    public async process() {
        const parameters = await RepairAvro.promptParameters();

        const awsS3 = new AWS.S3({signatureVersion: 'v2'});

        try {
            const values = await awsS3.listObjects({
                Bucket: parameters.bucket,
                Prefix: parameters.folder
            }).promise();

            if (!values.Contents?.length) {
                logger.info(`No files found.`);
                return;
            }

            Directory.createIfNotExists(`avro`);

            const progressBar = new ProgressBar(`Processing files`, `files`)
                .start(values.Contents?.length);

            for (const item of values.Contents) {
                const avroPath = await this.donwload(awsS3, parameters, item);

                if (!await this.analyze(avroPath)) {
                    let repaired = await this.repair(avroPath);
                    if (repaired) {
                        logger.info(`[${path.basename(avroPath)}] was repaired.`);
                        if (parameters.replace) {
                            await this.upload(awsS3, parameters, item);
                            logger.info(`[${path.basename(avroPath)}] corrupted version has replaced by new on the bucket.`);
                        }
                    } else {
                        logger.info(`[${path.basename(avroPath)}] cannot be repaired.`);
                    }
                } else {
                    Files.delete(avroPath);
                }

                progressBar.increment();
            }
        } catch (e: any) {
            logger.warn(`Aborted, because an error occurred.`, e);
        }

        logger.info(`Finished`);
    }

    private static async promptParameters(): Promise<any> {
        prompt.start();

        const schema = {
            properties: {
                bucket: {
                    message: 'S3 URI',
                    required: true,
                    default: 'prod-plat-datalake'
                },
                folder: {
                    message: 'Folder',
                    required: true,
                    default: 'extensoes/execucoes-metricas/data=2022-10-11'
                },
                replace: {
                    message: 'Replace on bucket (upload)',
                    required: true,
                    default: false,
                    type: 'boolean'
                }
            }
        };

        return await prompt.get(schema);
    }

    private async donwload(awsS3: S3, parameters: any, item: any): Promise<string> {
        const {Body} = await awsS3.getObject({
            Bucket: parameters.bucket,
            Key: item.Key!.toString()
        }).promise();

        const filename = path.basename(item.Key!.toString());
        await fs.writeFile(`avro/${filename}`, Body);

        return Path.join(process.cwd(), `avro/${filename}`);
    }

    private async upload(awsS3: S3, parameters: any, avro: any): Promise<ManagedUpload.SendData> {
        return await awsS3.upload({
            Bucket: `${parameters.bucket}/${parameters.folder}`,
            Key: path.basename(avro),
            Body: Files.getContent(avro)
        }).promise();
    }

    private async analyze(avroPath: string): Promise<boolean> {
        const outputFile = path.join(path.dirname(avroPath), `sample.${path.basename(avroPath)}`);

        try {
            const bash = util.promisify(require('child_process').exec);
            const {stderr} = await bash(`${this.baseAvroToolsCommandLine} cat --offset 0 --limit ${this.configuration.avro.limit} --samplerate ${this.configuration.avro.samplerate} ${avroPath} ${outputFile}`);
            if (stderr.length <= 0) {
                Files.delete([outputFile, path.join(path.dirname(avroPath), `.sample.${path.basename(avroPath)}.crc`)]);
                return true;
            }
            logger.warn(`[${path.basename(avroPath)}] ${stderr}`);
        } catch (e: any) {
            logger.error(`[${path.basename(avroPath)}] fail to export.`);
        }

        return false;
    }

    private async repair(avroPath: string): Promise<boolean> {
        const outputFile = path.join(path.dirname(avroPath), `repaired.${path.basename(avroPath)}`);

        try {
            const bash = util.promisify(require('child_process').exec);
            await bash(`${this.baseAvroToolsCommandLine} repair ${avroPath} ${outputFile}`);

            if (await this.analyze(outputFile)) {
                await Files.delete(avroPath);
                await Files.rename(outputFile, avroPath);

                return true;
            } else {
                await Files.delete(outputFile);
            }
        } catch (e: any) {
            logger.error(`[${path.basename(avroPath)}] cannot be repaired.`);
        }

        return false;
    }

}
