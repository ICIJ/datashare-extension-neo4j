// Follow: https://datatracker.ietf.org/doc/html/rfc9457
export class HTTPError extends Error {
  constructor(status, title, detail) {
    const message = `${title}
Detail: ${detail}
`
    super(message);
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, HTTPError);
    }
    this.name = "HTTPError";
    this.status = status
    this.title = title
    this.detail = detail
  }
}

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
      var error;
      try {
        const { title, detail } = await response.json()
        error = new HTTPError(response.status, title, detail)
      } catch (e) {
        if (e instanceof TypeError) {
          const textError = await response.text()
          throw new Error(textError)
        }
        throw e;
      }
      throw error
    }
    return response
  }

  getFullUrl(url) {
    const Api = this.#core.api.constructor
    const fullUrl = Api.getFullUrl(url)
    return fullUrl
  }

  async request(url, config = {}) {
    const fullUrl = this.getFullUrl(url)
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
