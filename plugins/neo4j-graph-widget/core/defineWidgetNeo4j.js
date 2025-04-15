import WidgetNeo4j from '@/components/WidgetNeo4j/WidgetNeo4j.vue'

export default (widgetEmptyCls) =>
  class extends widgetEmptyCls {
    get component() {
      return WidgetNeo4j
    }
  }
