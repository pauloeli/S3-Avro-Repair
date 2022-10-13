import AWS, {S3} from 'aws-sdk';
import * as path from 'path';
import {Directory} from '../helpers/directory';
import Path from 'path';
import * as util from 'util';
import {Files} from '../helpers/file';
import {ProgressBar} from '../helpers/progress-bar';

const prompt = require('prompt');
const logger = require('../helpers/logger');
const fs = require('fs').promises;

export class RepairAvro {

    private configuration: any;

    constructor(configuration: any) {
        this.configuration = configuration;

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
                Delimiter: '/',
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
                    await this.repair(avroPath)
                        ? logger.info(`[${path.basename(avroPath)}] was repaired.`)
                        : logger.info(`[${path.basename(avroPath)}] cannot be repaired.`);
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
                    default: 'extensoes/execucoes-metricas/data=2022-10-12/'
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

    private async analyze(avroPath: string, deleteIfSuccess: boolean = true): Promise<boolean> {
        const outputFile = path.join(path.dirname(avroPath), `${path.basename(avroPath)}.json`);

        try {
            const bash = util.promisify(require('child_process').exec);
            const {stdout} = await bash(`${this.configuration.spec.environment.java} -jar -Dlog4j.configuration=file:./bin/log4j.properties ./bin/avro-tools-1.8.2.jar tojson ${avroPath} > ${outputFile}`);
            if (stdout.length <= 0 && this.isValidJson(outputFile)) {
                return true;
            }
        } catch (e: any) {
            logger.error(`[${path.basename(avroPath)}] fail to export. Reason: ${e?.message || e}`);
        } finally {
            deleteIfSuccess && Files.deleteFiles([avroPath, outputFile]);
        }

        return false;
    }

    private async repair(avroPath: string): Promise<boolean> {
        const outputFile = path.join(path.dirname(avroPath), `repaired.${path.basename(avroPath)}`);

        try {
            const bash = util.promisify(require('child_process').exec);
            await bash(`${this.configuration.spec.environment.java} -jar -Dlog4j.configuration=file:./bin/log4j.properties ./bin/avro-tools-1.8.2.jar repair ${avroPath} ${outputFile}`);

            if (!await this.analyze(outputFile, false)) {
                Files.delete(outputFile);
                return false;
            } else {
                Files.delete(avroPath);
                Files.rename(outputFile, avroPath);

                return true;
            }
        } catch (e: any) {
            logger.error(`[${path.basename(avroPath)}] cannot be repaired. Reason: ${e?.message || e}`);
        }

        return false;
    }

    private isValidJson(filePath: string): boolean {
        try {
            return Files.getContent(filePath).length > 0;
        } catch (e) {
            return false;
        }
    }

}
