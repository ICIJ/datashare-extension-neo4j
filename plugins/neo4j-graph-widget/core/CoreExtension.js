export class CoreExtension {
  #api

  constructor(api) {
    this.#api = api
  }
  
  getFullUrl(url) {
    return this.#api.constructor.getFullUrl(url)
  }

  async request(url, config = {}) {
    const result = await this.#api.sendAction(url, config)
    return result
  }
}

export default CoreExtension
