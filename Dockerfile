###### First stage: build the executable.

# Start from golang:1.11-alpine base image
FROM golang:1.13-alpine As Builder

# enable go module
ENV GO111MODULE=on

# Create the user and group files that will be used in the running container to
# run the process as an unprivileged user.
RUN mkdir /user && \
    echo 'nobody:x:65534:65534:nobody:/:' > /user/passwd && \
    echo 'nobody:x:65534:' > /user/group

# Install the Certificate-Authority certificates for the app to be able to make
# calls to HTTPS endpoints.
# Git is required for fetching the dependencies.
RUN apk add --no-cache ca-certificates git

# Set the environment variables for the go command:
# * CGO_ENABLED=0 to build a statically-linked executable
# * GOFLAGS=-mod=vendor to force `go build` to look into the `/vendor` folder.
ENV CGO_ENABLED=0
# ENV GOFLAGS=-mod=vendor //use it when your packages in a vendor folder in project root

# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/github.com/Mekawy5/BitCollect

# caching go dependencies
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . .

# Build the executable to `/app`. Mark the build as statically linked.
RUN go build \
    -installsuffix 'static' \
    -o /app ./cmd

#========================================================
###### Final stage: the running container.
FROM scratch AS final

# Import the user and group files from the first stage.
COPY --from=Builder /user/group /user/passwd /etc/

# Import the Certificate-Authority certificates for enabling HTTPS.
COPY --from=Builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Import the compiled executable from the second stage.
COPY --from=Builder /app /app

# Declare the port on which the webserver will be exposed.
# As we're going to run the executable as an unprivileged user, we can't bind
# to ports below 1024.
EXPOSE 8080

# Perform any further action as an unprivileged user.
USER nobody:nobody

# Run the compiled binary.
ENTRYPOINT ["/app"]
