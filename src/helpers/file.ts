import * as _ from 'lodash';

const fs = require('fs');

export class Files {

    public static getContent(path: string): any {
        return fs.readFileSync(path);
    }

    public static getContentWithEncoding(path: string, encoding: BufferEncoding = 'utf-8'): any {
        return fs.readFileSync(path, encoding);
    }

    public static delete(value: Array<string> | string) {
        _.isArray(value) ? value.forEach((e: string) => fs.unlinkSync(e)) : fs.unlinkSync(value);
    }

    public static rename(from: string, to: string) {
        fs.renameSync(from, to);
    }

}
