package com.siemens.opc.server;

import com.google.common.collect.ImmutableList;
import com.siemens.opc.server.milo.KeyStoreLoader;
import com.siemens.opc.server.milo.Namespace;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.identity.CompositeValidator;
import org.eclipse.milo.opcua.sdk.server.identity.UsernameIdentityValidator;
import org.eclipse.milo.opcua.sdk.server.identity.X509IdentityValidator;
import org.eclipse.milo.opcua.sdk.server.util.HostnameUtil;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.application.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.application.DirectoryCertificateValidator;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.structured.BuildInfo;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;
import org.eclipse.milo.opcua.stack.core.util.CryptoRestrictions;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.Security;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Lists.newArrayList;
import static org.eclipse.milo.opcua.sdk.server.api.config.OpcUaServerConfig.*;
import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.*;

public class OpcServerApp {

    static {
        CryptoRestrictions.remove();

        // Required for SecurityPolicy.Aes256_Sha256_RsaPss
        Security.addProvider(new BouncyCastleProvider());
    }

    public static void main(String[] args) throws Exception {
        OpcServerApp server = new OpcServerApp();

        server.startup().get();

        final CompletableFuture<Void> future = new CompletableFuture<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> future.complete(null)));

        future.get();
    }

    private OpcUaServer server;

    public OpcServerApp() throws Exception {
        Properties opcServerProperties = PropertyUtil.readPropertiesFromFile("C:\\SiemensProjects\\OpcServer\\src\\main\\resources\\opcserver.properties");

        File securityTempDir = new File(System.getProperty("java.io.tmpdir"), "security");
        if (!securityTempDir.exists() && !securityTempDir.mkdirs()) {
            throw new Exception("unable to create security temp dir: " + securityTempDir);
        }
        LoggerFactory.getLogger(getClass()).info("security temp dir: {}", securityTempDir.getAbsolutePath());

        KeyStoreLoader loader = new KeyStoreLoader().load(securityTempDir);

        DefaultCertificateManager certificateManager = new DefaultCertificateManager(
                loader.getServerKeyPair(),
                loader.getServerCertificateChain()
        );

        File pkiDir = securityTempDir.toPath().resolve("pki").toFile();
        DirectoryCertificateValidator certificateValidator = new DirectoryCertificateValidator(pkiDir);
        LoggerFactory.getLogger(getClass()).info("pki dir: {}", pkiDir.getAbsolutePath());

        UsernameIdentityValidator identityValidator = new UsernameIdentityValidator(
                true,
                authChallenge -> {
                    String username = authChallenge.getUsername();
                    String password = authChallenge.getPassword();

                    boolean userOk = "user".equals(username) && "password1".equals(password);
                    boolean adminOk = "admin".equals(username) && "password2".equals(password);

                    return userOk || adminOk;
                }
        );

        X509IdentityValidator x509IdentityValidator = new X509IdentityValidator(c -> true);

        List<String> bindAddresses = newArrayList();
        bindAddresses.add("0.0.0.0");

        List<String> endpointAddresses = newArrayList();
        endpointAddresses.add(HostnameUtil.getHostname());
        endpointAddresses.addAll(HostnameUtil.getHostnames("0.0.0.0"));

        // The configured application URI must match the one in the certificate(s)
        String applicationUri = certificateManager.getCertificates().stream()
                .findFirst()
                .map(certificate ->
                        CertificateUtil.getSubjectAltNameField(certificate, CertificateUtil.SUBJECT_ALT_NAME_URI)
                                .map(Object::toString)
                                .orElseThrow(() -> new RuntimeException("certificate is missing the application URI")))
                .orElse("urn:siemens:opc:server:" + UUID.randomUUID());

        OpcUaServerConfig serverConfig = OpcUaServerConfig.builder()
                .setApplicationUri(applicationUri)
                .setApplicationName(LocalizedText.english(opcServerProperties.getProperty("applicationName")))
                .setBindPort(Integer.parseInt(opcServerProperties.getProperty("applicationPort")))
                .setBindAddresses(bindAddresses)
                .setEndpointAddresses(endpointAddresses)
                .setBuildInfo(
                        new BuildInfo(
                                "urn:siemens:opc:server",
                                "siemens",
                                "siemens opc test server",
                                OpcUaServer.SDK_VERSION,
                                "", DateTime.now()))
                .setCertificateManager(certificateManager)
                .setCertificateValidator(certificateValidator)
                .setIdentityValidator(new CompositeValidator(identityValidator, x509IdentityValidator))
                .setProductUri("urn:siemens:opc:server")
                .setServerName(opcServerProperties.getProperty("serverName"))
                .setSecurityPolicies(
                        EnumSet.of(
                                SecurityPolicy.None,
                                SecurityPolicy.Basic128Rsa15,
                                SecurityPolicy.Basic256,
                                SecurityPolicy.Basic256Sha256,
                                SecurityPolicy.Aes128_Sha256_RsaOaep,
                                SecurityPolicy.Aes256_Sha256_RsaPss))
                .setUserTokenPolicies(
                        ImmutableList.of(
                                USER_TOKEN_POLICY_ANONYMOUS,
                                USER_TOKEN_POLICY_USERNAME,
                                USER_TOKEN_POLICY_X509))
                .build();

        server = new OpcUaServer(serverConfig);

        List<ScalarNode> scalarNodes = new ArrayList<>();
        scalarNodes.add(new ScalarNode("Boolean", Identifiers.Boolean, new Variant(false)));
        scalarNodes.add(new ScalarNode("Byte", Identifiers.Byte, new Variant(0xCC)));
        scalarNodes.add(new ScalarNode("SByte", Identifiers.SByte, new Variant(0xFF)));
        scalarNodes.add(new ScalarNode("Int16", Identifiers.Int16, new Variant(16)));
        scalarNodes.add(new ScalarNode("Int32", Identifiers.Int32, new Variant(32)));
        scalarNodes.add(new ScalarNode("Int64", Identifiers.Int64, new Variant(64L)));
        scalarNodes.add(new ScalarNode("UInt16", Identifiers.UInt16, new Variant(ushort(16))));
        scalarNodes.add(new ScalarNode("UInt32", Identifiers.UInt32, new Variant(uint(32))));
        scalarNodes.add(new ScalarNode("UInt64", Identifiers.UInt64, new Variant(ulong(64L))));
        scalarNodes.add(new ScalarNode("Float", Identifiers.Float, new Variant(3.14f)));
        scalarNodes.add(new ScalarNode("Double", Identifiers.Double, new Variant(3.14d)));
        scalarNodes.add(new ScalarNode("String", Identifiers.String, new Variant("string value")));
        scalarNodes.add(new ScalarNode("DateTime", Identifiers.DateTime, new Variant(DateTime.now())));
        scalarNodes.add(new ScalarNode("Guid", Identifiers.Guid, new Variant(UUID.randomUUID())));
        scalarNodes.add(new ScalarNode("ByteString", Identifiers.ByteString, new Variant(new ByteString(new byte[]{0x01, 0x02, 0x03, 0x04}))));
        scalarNodes.add(new ScalarNode("XmlElement", Identifiers.XmlElement, new Variant(new XmlElement("<a>hello</a>"))));
        scalarNodes.add(new ScalarNode("LocalizedText", Identifiers.LocalizedText, new Variant(LocalizedText.english("localized text"))));
        scalarNodes.add(new ScalarNode("QualifiedName", Identifiers.QualifiedName, new Variant(new QualifiedName(1234, "defg"))));
        scalarNodes.add(new ScalarNode("NodeId", Identifiers.NodeId, new Variant(new NodeId(1234, "abcd"))));
        scalarNodes.add(new ScalarNode("Duration", Identifiers.Duration, new Variant(1.0)));
        scalarNodes.add(new ScalarNode("UtcTime", Identifiers.UtcTime, new Variant(DateTime.now())));

        server.getNamespaceManager().registerAndAdd(
                "urn:siemens:opc:server:simulation",
                idx -> new Namespace(server, "urn:siemens:opc:server:simulation", idx,"Simulation", "ScalarNodes", scalarNodes));

        server.getNamespaceManager().registerAndAdd(
                "urn:siemens:opc:server:simulation2",
                idx -> new Namespace(server, "urn:siemens:opc:server:simulation2", idx, "Simulation2", "ScalarNodes", scalarNodes));
    }

    public OpcUaServer getServer() {
        return server;
    }

    public CompletableFuture<OpcUaServer> startup() {
        return server.startup();
    }

    public CompletableFuture<OpcUaServer> shutdown() {
        return server.shutdown();
    }

}
