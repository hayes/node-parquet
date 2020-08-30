module.exports = {
  rules: {
    'sort-keys': 'off',
    'no-use-before-define': 'off',
    'no-restricted-syntax': 'off',
    '@typescript-eslint/no-use-before-define': ['error', { functions: false, classes: true }],
    'lines-between-class-members': ['error', 'always', { exceptAfterSingleLine: true }],
    'no-magic-numbers': 'off',
    'no-bitwise': 'off',
    'no-eq-null': 'off',
    'unicorn/number-literal-case': 'off',
    'no-plusplus': 'off',
  },
  overrides: [
    {
      files: ['./tests/**/*.ts'],
      parserOptions: {
        project: './tests/tsconfig.json',
      },
    },
  ],
};
