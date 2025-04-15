// Follow: https://datatracker.ietf.org/doc/html/rfc9457
export class HTTPError extends Error {
  constructor(status, title, detail) {
    const message = `${title}\nDetail: ${detail}\n`
    super(message)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, HTTPError)
    }
    this.name = 'HTTPError'
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

  static async handleAppError(response) {
    if (!response.ok) {
      let error
      try {
        const { title, detail } = await response.json()
        error = new HTTPError(response.status, title, detail)
      } catch (e) {
        if (e instanceof TypeError) {
          const textError = await response.text()
          throw new Error(textError)
        }
        throw e
      }
      throw error
    }
    return response
  }

  getFullUrl(url) {
    return this.#core.api.constructor.getFullUrl(url)
  }

  async request(url, config = {}) {
    const fullUrl = this.getFullUrl(url)
    try {
      const body = JSON.stringify(config.data)
      const result = await fetch(fullUrl, { body, ...config })
      return CoreExtension.handleAppError(result)
    } catch (error) {
      this.#core.emit('http::error', error)
      throw error
    }
  }
}

export default CoreExtension
