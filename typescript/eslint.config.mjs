// Rules follow temporalio/ai-integrations typescript/_shared/eslint.config.mjs.
import js from '@eslint/js';
import prettierConfig from 'eslint-config-prettier';
import importPlugin from 'eslint-plugin-import';
import { defaultConditionNames } from 'eslint-import-resolver-typescript';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  { ignores: ['**/node_modules/**', '**/lib/**', '**/dist/**', '**/*.js', '**/*.mjs', '**/*.cjs'] },
  {
    files: ['src/**/*.ts'],
    extends: [js.configs.recommended, ...tseslint.configs.recommended, prettierConfig],
    plugins: { import: importPlugin },
    languageOptions: {
      parserOptions: { project: ['./tsconfig.json'], tsconfigRootDir: import.meta.dirname },
    },
    settings: {
      'import/parsers': { '@typescript-eslint/parser': ['.ts'] },
      'import/resolver': {
        typescript: {
          alwaysTryTypes: true,
          project: ['./tsconfig.json'],
          // The MCP SDK wildcard types export maps `.js` imports to `.js.d.ts` files that do
          // not exist. Resolve the runtime exports. Extension aliases still find the
          // declarations.
          conditionNames: defaultConditionNames.filter((condition) => condition !== 'types'),
        },
      },
      'import/internal-regex': '^@temporalio/',
    },
    rules: {
      eqeqeq: ['error', 'always', { null: 'ignore' }],
      'no-duplicate-imports': ['error', { allowSeparateTypeImports: true }],
      'object-shorthand': ['error', 'always'],
      '@typescript-eslint/no-deprecated': 'warn',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/explicit-module-boundary-types': 'error',
      '@typescript-eslint/consistent-type-imports': [
        'error',
        { prefer: 'type-imports', fixStyle: 'separate-type-imports', disallowTypeAnnotations: false },
      ],
      '@typescript-eslint/no-unused-vars': [
        'warn',
        { argsIgnorePattern: '^_', caughtErrorsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      'import/no-unresolved': 'error',
      'import/named': 'off',
      'import/default': 'error',
      'import/namespace': 'error',
      'import/no-absolute-path': 'error',
      'import/no-self-import': 'error',
      'import/no-cycle': 'error',
      'import/no-useless-path-segments': 'error',
      'import/no-relative-packages': 'error',
      'import/export': 'error',
      'import/no-named-as-default': 'error',
      'import/no-extraneous-dependencies': 'error',
      'import/no-mutable-exports': 'error',
      'import/unambiguous': 'error',
      'import/first': 'error',
      'import/order': ['error', { groups: ['builtin', 'external', 'internal', 'parent', 'sibling', 'index'] }],
      'import/newline-after-import': 'error',
      'import/no-unassigned-import': 'error',
      'import/no-named-default': 'error',
    },
  },
  {
    files: ['src/**/*.ts'],
    ignores: ['src/__tests__/**'],
    rules: {
      'no-restricted-imports': ['error', { patterns: ['**/__tests__/**', '**/*.test'] }],
    },
  },
);
