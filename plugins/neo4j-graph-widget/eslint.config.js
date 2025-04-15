import compat from './bin/eslint/compat.js'

export default [
  {
    ignores: ['dist', 'node_modules']
  },
  ...compat.extends('@icij/eslint-config-icij'),
  {
    rules: {
      'no-unused-vars': ['error', { argsIgnorePattern: '^(Ph[A-Z])|(_)' }],
      'vue/no-v-model-argument': 'off',
      'vue/no-v-for-template-key': 'off',
      'vue/no-custom-modifiers-on-v-model': 'off',
      'vue/no-multiple-template-root': 'off',
      'vue/valid-v-slot': 'off',
      'import/extensions': 'off'
    }
  }
]
