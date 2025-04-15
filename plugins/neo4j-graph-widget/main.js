import defineWidgetNeo4jGraph from './core/defineWidgetNeo4j'

document.addEventListener(
  'datashare:ready',
  async ({ detail }) => {
    detail.core.registerWidget({
      name: 'WidgetNeo4j',
      section: 'graph',
      order: 0,
      type: defineWidgetNeo4jGraph
    })
  },
  false
)
