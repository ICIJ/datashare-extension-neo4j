import uniqueId from 'lodash/uniqueId'

class Neo4jDumpCondition {
  /**
   * // TODO
   */
  constructor({ name = uniqueId('neo4j-where-'), variableName, properties = [] } = {}) {
    this.name = name
    this.variableName = variableName
    this.properties = properties
    this.conditionName = null
    this.type = null
    this.property = null
    this.value = null
    this.not = false
  }

  toWhere() {
    // TODO: return if we don't have the condition name or property or value
    let where = {}
    where[this.conditionName] = {
      property: {
        name: this.variableName,
        variable: this.property,
      },
      value: {
        "literal": this.value
      }
    }
    if (this.not) {
      where = { not: where }
    }
    return where
  }
}

export default Neo4jDumpCondition
