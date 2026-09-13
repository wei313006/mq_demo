FROM eclipse-temurin:17-jre
WORKDIR /app
# 注意这里：因为你是多模块，这里以 provider 模块为例，请根据你要部署的模块修改路径
COPY provider/target/provider-0.0.1-SNAPSHOT.jar provider.jar
ENTRYPOINT ["java", "-jar", "provider.jar"]