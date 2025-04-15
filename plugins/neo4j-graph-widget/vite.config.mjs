import path, { resolve } from 'path'
import { defineConfig, loadEnv } from 'vite'
import { viteExternalsPlugin } from 'vite-plugin-externals'
import { BootstrapVueNextResolver } from 'unplugin-vue-components/resolvers'
import Components from 'unplugin-vue-components/vite'
import vue from '@vitejs/plugin-vue'

export default ({ mode }) => {
  process.env = Object.assign(process.env, loadEnv(mode, process.cwd(), ''))

  return defineConfig({
    plugins: [
      vue(),
      viteExternalsPlugin({
        vue: '__VUE_SHARED__',
        pinia: '__PINIA_SHARED__'
      }),
      Components({
        dts: false,
        dirs: [],
        resolvers: [BootstrapVueNextResolver()]
      })
    ],
    resolve: {
      dedupe: ['vue'],
      extensions: ['.mjs', '.js', '.mts', '.ts', '.jsx', '.tsx', '.json', '.vue'],
      alias: {
        '@': resolve(__dirname)
      }
    },
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
