FROM alpine:3.18.0

RUN apk --no-cache upgrade
RUN apk --no-cache add ca-certificates tzdata zsh jq tmux curl

RUN addgroup -g 1000 logferry && adduser -u 1000 -D -G logferry logferry
RUN touch ~logferry/.zshrc ~root/.zshrc; chown logferry:logferry ~logferry/.zshrc

RUN mkdir /app
ADD dist/logferry-api_linux_amd64_v1/logferry-api /app/
ADD dist/logferry_linux_amd64_v1/logferry /app/

EXPOSE 4200
EXPOSE 4290

USER logferry

# Container start command for production
CMD ["/app/logferry-api"]
