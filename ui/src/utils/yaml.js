import YAML from 'yaml'
import yaml from 'js-yaml'

export function jsonToYaml(jsonData) {
    try {
        return {
            data: typeof (jsonData) == 'string' ? yaml.dump(JSON.parse(jsonData)) : yaml.dump(jsonData),
            error: false
        }
    } catch (err) {
        return {
            data: '',
            error: true
        }
    }
}

export function yamlToJson(yamlStr, returnString) {
    try {
        return {
            data: returnString ? JSON.stringify(YAML.parse(yamlStr), null, 2) : YAML.parse(yamlStr),
            error: false
        }
    } catch (err) {
        return {
            data: '',
            error: true
        }
    }
}