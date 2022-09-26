export class Validators {

    public static requireNonNull(value: any, message: string) {
        if (!value) {
            throw new Error(message);
        }
    }

    public static requireMultiNonNull(values: any[], message: string) {
        values.forEach(e => this.requireNonNull(e, message));
    }

}
