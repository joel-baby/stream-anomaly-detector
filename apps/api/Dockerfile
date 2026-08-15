FROM node:22-alpine AS build
WORKDIR /app

COPY package.json pnpm-workspace.yaml ./
RUN npm install -g pnpm && pnpm install

COPY tsconfig.json ./
COPY src ./src
RUN pnpm build

FROM node:22-alpine AS run
WORKDIR /app

COPY package.json pnpm-workspace.yaml ./
RUN npm install -g pnpm && pnpm install --prod

COPY --from=build /app/dist ./dist

CMD ["node", "dist/index.js"]