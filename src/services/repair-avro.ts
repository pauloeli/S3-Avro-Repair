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
                const path = await this.donwload(awsS3, parameters, item);
                await this.analyze(path);

                progressBar.increment();
            }
        } catch (e: any) {
            logger.warn(`File: ${parameters.name} ignored, because an error occurred.`, e);
        }

        logger.info(`Process ended`);
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
                    default: 'extensoes/execucoes-metricas/data=2022-10-03/'
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

    private async analyze(filePath: string, tryRepair: boolean = true): Promise<boolean> {
        const outputFile = path.join(path.dirname(filePath), `${path.basename(filePath)}.json`);

        try {
            const bash = util.promisify(require('child_process').exec);
            const {stdout} = await bash(`${this.configuration.spec.environment.java} -jar -Dlog4j.configuration=file:./bin/log4j.properties ./bin/avro-tools-1.8.2.jar tojson ${filePath} > ${outputFile}`);
            if (stdout.length <= 0 && this.isValidJson(outputFile)) {
                return Promise.resolve(true);
            }
        } catch (e) {
            logger.error(`Fail to export ${path.basename(filePath)}.`);
        }

        return tryRepair ? this.repair(filePath) : Promise.resolve(false);
    }

    private async repair(filePath: string): Promise<boolean> {
        const outputFile = path.join(path.dirname(filePath), `repaired.${path.basename(filePath)}`);

        try {
            const bash = util.promisify(require('child_process').exec);
            await bash(`${this.configuration.spec.environment.java} -jar -Dlog4j.configuration=file:./bin/log4j.properties ./bin/avro-tools-1.8.2.jar repair ${filePath} ${outputFile}`);

            if (!await this.analyze(outputFile, false)) {
                this.deleteFiles([filePath, outputFile]);

                logger.warn(`File ${filePath} cannot be repaired.`);
            } else {
                logger.info(`File ${filePath} was repaired.`);
                return Promise.resolve(true);
            }
        } catch (e) {
            logger.error(`File ${path.basename(filePath)} cannot be repaired. Reason: ${e}`);
        }

        return Promise.resolve(false);
    }

    private isValidJson(filePath: string): boolean {
        try {
            return Files.getContent(filePath).length > 0;
        } catch (e) {
            return false;
        }
    }

    private deleteFiles(patches: Array<string>) {
        try {
            patches.forEach((path: string) => {
                if (fs.exists(path)) {
                    fs.unlinkSync(path);
                }
            });
        } catch (e) {
        }
    }

}
