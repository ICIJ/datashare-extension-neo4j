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
  #api

  constructor(api) {
    this.#api = api
  }

  async request(url, config = {}) {
    const body = JSON.stringify(config.data)
    const result = await this.#api.sendAction(url, { body, ...config })
    return result
  }
}

export default CoreExtension
