import {RepairAvro} from './services/repair-avro';
import {Validators} from './helpers/validators';
import {Yaml} from './helpers/yaml';

(async () => {
    Validators.requireMultiNonNull([process.env?.AWS_ACCESS_KEY_ID, process.env?.AWS_SECRET_ACCESS_KEY, process.env?.AWS_REGION],
        'It is mandatory to provide access credentials');

    await new RepairAvro(Yaml.getConfig()?.spec).process();

})();
