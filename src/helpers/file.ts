import * as fs from 'fs';
import * as Path from 'path';
import * as _ from 'lodash';

export class Files {

    public static async createFile(path: string, content: any, indent: boolean = false) {
        const directory = Path.dirname(Path.join(process.cwd(), path));

        if (!fs.existsSync(directory)) {
            fs.mkdirSync(directory, {recursive: true});
        }

        await fs.writeFileSync(Path.join(process.cwd(), path),
            _.isObject(content) ? JSON.stringify(content, null, indent ? 2 : 0) : content, {encoding: 'utf-8'});
    }

    public static getContent(path: string, encoding: BufferEncoding = 'utf-8'): any {
        return fs.readFileSync(path, encoding);
    }

    public static getContentAs<T>(path: string, encoding: BufferEncoding = 'utf-8'): T | null {
        const value = this.getContent(path, encoding);
        return value ? JSON.parse(value) as T : null;
    }

    public static exists(path: string): boolean {
        return fs.existsSync(path);
    }

    public static filesInDirectory(path: string): Array<string> {
        return fs.readdirSync(path);
    }

    public static delete(path: string): void {
        if (this.exists(path)) {
            fs.unlinkSync(path);
        }
    }

}
