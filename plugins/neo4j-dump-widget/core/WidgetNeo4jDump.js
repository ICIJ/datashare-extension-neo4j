import Component from '../components/WidgetNeo4jDump.vue'

export default (widgetEmptyCls) => class WidgetNeo4jDump extends widgetEmptyCls {
  get component() {
    return Component
  }
}
