import Component from '../components/WidgetNeo4jGraph.vue'

export default (widgetEmptyCls) => class WidgetNeo4jGraph extends widgetEmptyCls {
  get component() {
    return Component
  }
}
