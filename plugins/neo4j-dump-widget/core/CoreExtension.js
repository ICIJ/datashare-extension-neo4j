export class CoreExtension {
  #core

  constructor(core) {
    this.#core = core
  }

  useExtension() {
    const neo4jCore = this
    this.#core.use(
      class VueCoreNeo4jExtension {
        static install(Vue) {
          Vue.prototype.$neo4jCore = neo4jCore
        }
      }
    )
  }

  static async handleAppError(response) {
    if (!response.ok) {
      var error = null;
      try {
        const { title, detail, trace } = await response.json()
        error = this.formatAppError(title, detail, trace)
      } catch (e) {
        if (e instanceof TypeError) {
          error = await response.text()
        } else {
          throw e;
        }
      }
      throw new Error(error)
    }
    return response
  }

  static formatAppError(title, detail, trace) {
    const formatted = `${title}
Detail: ${detail}
`
    return trace ? `${formatted}\n${trace}` : formatted
  }

  async request(url, config = {}) {
    const ApiClass = this.#core.api.constructor
    const fullUrl = ApiClass.getFullUrl(url)
    try {
      const r = await fetch(fullUrl, {
        body: JSON.stringify(config.data),
        ...config
      });
      return CoreExtension.handleAppError(r)
    } catch (error) {
      this.#core.api.eventBus?.$emit('http::error', error)
      throw error
    }
  }

}

export default CoreExtension
