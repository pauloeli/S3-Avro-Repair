import yaml from 'js-yaml';
import {Files} from './file';

export class Yaml {

    public static getConfig(environment: any = null): any {
        if (environment) {
            return yaml.load(Files.getContent(`resources/application-${environment}.yml`));
        }

        return yaml.load(Files.getContent('resources/application.yml'));
    }

}
