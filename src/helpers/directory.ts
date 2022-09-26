import * as fs from 'fs';

export class Directory {

    public static createIfNotExists(path: string) {
        if (this.exists(path)) {
            return;
        }

        fs.mkdirSync(path, {recursive: true});
    }

    public static exists(path: string): boolean {
        return fs.existsSync(path);
    }

}
