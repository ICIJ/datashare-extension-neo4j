export class Utils {
  static async sendActionAsRaw(url, config = {}) {
    const fullUrl = this.getFullUrl(`/api/neo4j${url}`)
    try {
      const r = await fetch(fullUrl, {
        body: JSON.stringify(config.data),
        ...config
      });
      return r
    } catch (error) {
      this.$core.api.eventBus?.$emit('http::error', error)
      throw error
    }
  }
  // This is static in the Api, we can't access it through this.$core.api
  static getFullUrl(path) {
    const base = `${window.location.protocol}//${window.location.host}`
    const url = new URL(path, base)
    return url.href
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
    return `${title}
Detail: ${detail}
Trace: ${trace}
`
  }
  static async request(url, config = {}) {
    const response = await this.sendActionAsRaw(url, config)
    return await this.handleAppError(response)
  }
}

export default Utils