import {createLogger, format, transports} from 'winston';
import _ from 'lodash';

const logger = createLogger({
    transports: [new transports.Console()],
    format: format.combine(
        format.colorize(),
        format.splat(),
        format.metadata(),
        format.timestamp(),
        format.printf(({timestamp, level, message}) => {
            return `[${timestamp}] ${_.capitalize(level)}: ${message}.`;
        })
    ),
});

module.exports = logger;
