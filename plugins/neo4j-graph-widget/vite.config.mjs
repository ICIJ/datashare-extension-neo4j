// This configuration file for Vite sets up the plugin environment to seamlessly integrate with a Vue 3 application 
// that uses a shared global Vue instance (`__VUE_SHARED__`) and a Vuex instance (`__VUEX_SHARED__`). 
// This approach is essential for ensuring that the plugin utilizes the same Vue instance as the main application, 
// thereby maintaining consistency and avoiding potential conflicts.

// We use the `vite-plugin-externals` plugin to externalize the Vue module. Instead of bundling Vue with the plugin, 
// the plugin will reference instances from the global `window` object, which is provided by the host application.
import { defineConfig, loadEnv } from 'vite'
import path from 'path'
import vue from '@vitejs/plugin-vue'
import { viteExternalsPlugin } from 'vite-plugin-externals'

export default ({ mode }) => {
  process.env = Object.assign(process.env, loadEnv(mode, process.cwd(), ''))
  
  return defineConfig({
    plugins: [    
      vue(),
       // Map Vue imports to the global `__VUE_SHARED__` and `__VUEX_SHARED__` object on from `window`.
      viteExternalsPlugin({ 
        vue: '__VUE_SHARED__', 
        vuex: '__VUEX_SHARED__' 
      }),
    ],
    build: {
      sourcemap: mode === 'development',
      lib: {
        entry: path.resolve(__dirname, 'main.js'),
        name: 'index',
        fileName: 'index'
      }
    }
  })
}