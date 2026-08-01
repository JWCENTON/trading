ARG BUILD_MODE=development
ARG GIT_SHA=DEV-UNKNOWN
ARG BUILD_TIMESTAMP=DEV-UNKNOWN

# Build stage
FROM node:20 AS build
ARG BUILD_MODE
ARG GIT_SHA
ARG BUILD_TIMESTAMP
RUN set -eu; \
    if [ "$BUILD_MODE" = "candidate" ]; then \
      printf '%s' "$GIT_SHA" | grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'; \
      printf '%s' "$BUILD_TIMESTAMP" | grep -Eq \
        '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'; \
    elif [ "$BUILD_MODE" = "development" ]; then \
      test "$GIT_SHA" = "DEV-UNKNOWN" || \
        printf '%s' "$GIT_SHA" | grep -Eq '^([0-9a-f]{40}|[0-9a-f]{64})$'; \
      test "$BUILD_TIMESTAMP" = "DEV-UNKNOWN" || \
        printf '%s' "$BUILD_TIMESTAMP" | grep -Eq \
          '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'; \
    else \
      echo "unsupported BUILD_MODE=$BUILD_MODE" >&2; exit 1; \
    fi

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

ARG VITE_API_BASE_URL
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

RUN printf '{"git_sha":"%s","build_timestamp":"%s"}\n' \
    "$GIT_SHA" "$BUILD_TIMESTAMP" > public/build-info.json

RUN npm run build

FROM nginx:stable

ARG GIT_SHA
ARG BUILD_TIMESTAMP
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.created="${BUILD_TIMESTAMP}"
LABEL org.opencontainers.image.version="${GIT_SHA}"

COPY --from=build /app/dist /usr/share/nginx/html
COPY ./docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY ./docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
