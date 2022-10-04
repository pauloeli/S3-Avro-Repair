const colors = require('colors');
const progress = require('cli-progress');

export class ProgressBar {

    private _progressBar: any;

    constructor(summary: string, type: string = '') {
        this._progressBar = new progress.SingleBar({
            format: `${summary} |` + colors.cyan('{bar}') + `| {percentage}% || {value}/{total} ${type}.`,
            barCompleteChar: '\u2588',
            barIncompleteChar: '\u2591',
            stopOnComplete: true,
            linewrap: null,
        });
    }

    public start(value: number): ProgressBar {
        this._progressBar.start(value, 0);
        return this;
    }

    public increment(): ProgressBar {
        this._progressBar.increment();
        return this;
    }

}
