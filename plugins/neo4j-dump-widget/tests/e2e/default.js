const { main } = require('../../package.json')

module.exports = {
  'default configuration test' : function (browser) {
    browser.url('http://localhost:8008')
    // Wait for the app to be started
    browser.waitForElementVisible('.landing')
    browser.assert.titleContains('Datashare')
    browser.expect.element(`script[src="/plugins/package/${main}"]`).to.be.present
    browser.expect.element('.landing__plugin').to.be.present
    browser.expect.element('.landing__plugin').text.to.startWith('This alert was created by `datashare-plugin-neo4j-dump-widget` plugin.')
  }
};
