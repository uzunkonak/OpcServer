package com.siemens.opc.server.milo;

import com.google.common.collect.Lists;
import com.siemens.opc.server.ScalarNode;
import com.siemens.opc.server.milo.methods.SqrtMethod;
import com.siemens.opc.server.milo.types.CustomDataType;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.core.Reference;
import org.eclipse.milo.opcua.sdk.core.ValueRank;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.api.AccessContext;
import org.eclipse.milo.opcua.sdk.server.api.DataItem;
import org.eclipse.milo.opcua.sdk.server.api.MethodInvocationHandler;
import org.eclipse.milo.opcua.sdk.server.api.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.api.nodes.VariableNode;
import org.eclipse.milo.opcua.sdk.server.model.nodes.variables.AnalogItemNode;
import org.eclipse.milo.opcua.sdk.server.nodes.*;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.AttributeDelegate;
import org.eclipse.milo.opcua.sdk.server.nodes.delegates.AttributeDelegateChain;
import org.eclipse.milo.opcua.sdk.server.util.AnnotationBasedInvocationHandler;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.OpcUaBinaryDataTypeDictionary;
import org.eclipse.milo.opcua.stack.core.types.OpcUaDataTypeManager;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.Range;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.WriteValue;
import org.eclipse.milo.opcua.stack.core.util.FutureUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.*;

public class Namespace implements org.eclipse.milo.opcua.sdk.server.api.Namespace {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Random random = new Random();

    private final SubscriptionModel subscriptionModel;

    private final NodeFactory nodeFactory;

    private final OpcUaServer server;
    private final String namespaceUri;
    private final String namespaceName;
    private final UShort namespaceIndex;
    private final String nodePath;
    private List<ScalarNode> scalarNodes;

    public Namespace(OpcUaServer server, String namespaceUri, UShort namespaceIndex, String namespaceName, String nodePath, List<ScalarNode> scalarNodes) {
        this.server = server;
        this.namespaceUri = namespaceUri;
        this.namespaceIndex = namespaceIndex;
        this.namespaceName = namespaceName;
        this.scalarNodes = scalarNodes;
        this.nodePath = nodePath;

        subscriptionModel = new SubscriptionModel(server, this);

        nodeFactory = new NodeFactory(
            server.getNodeMap(),
            server.getObjectTypeManager(),
            server.getVariableTypeManager()
        );

        try {
            NodeId folderNodeId = new NodeId(namespaceIndex, namespaceName);

            UaFolderNode folderNode = new UaFolderNode(
                server.getNodeMap(),
                folderNodeId,
                new QualifiedName(namespaceIndex, namespaceName),
                LocalizedText.english(namespaceName)
            );

            server.getNodeMap().addNode(folderNode);

            // Make sure our new folder shows up under the server's Objects folder
            server.getUaNamespace().addReference(
                Identifiers.ObjectsFolder,
                Identifiers.Organizes,
                true,
                folderNodeId.expanded(),
                NodeClass.Object
            );

            // Add the rest of the nodes
            addVariableNodes(nodePath, folderNode);

            addMethodNode(folderNode);

            addCustomDataTypeVariable(folderNode);

            addCustomObjectTypeAndInstance(folderNode);
        } catch (UaException e) {
            logger.error("Error adding nodes: {}", e.getMessage(), e);
        }
    }

    @Override
    public UShort getNamespaceIndex() {
        return namespaceIndex;
    }

    @Override
    public String getNamespaceUri() {
        return namespaceUri;
    }

    private void addVariableNodes(String nodePath, UaFolderNode rootNode) {
        addScalarNodes(nodePath, rootNode);
    }

    private void addScalarNodes(String nodeName, UaFolderNode rootNode) {
        UaFolderNode scalarTypesFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/" + nodeName),
            new QualifiedName(namespaceIndex, "ScalarTypes"),
            LocalizedText.english("ScalarTypes")
        );

        server.getNodeMap().addNode(scalarTypesFolder);
        rootNode.addOrganizes(scalarTypesFolder);

        for (ScalarNode scalarNode : scalarNodes) {
            String name = scalarNode.getNodeName();
            NodeId typeId = scalarNode.getNodeId();
            Variant variant = scalarNode.getVariant();

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
                .setNodeId(new NodeId(namespaceIndex, namespaceName+ "/" + nodeName + "/" + name))
                .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
                .setUserAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
                .setBrowseName(new QualifiedName(namespaceIndex, name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            node.setAttributeDelegate(new ValueLoggingDelegate());

            server.getNodeMap().addNode(node);
            scalarTypesFolder.addOrganizes(node);
        }
    }

    private void addWriteOnlyNodes(UaFolderNode rootNode) {
        UaFolderNode writeOnlyFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/WriteOnly"),
            new QualifiedName(namespaceIndex, "WriteOnly"),
            LocalizedText.english("WriteOnly")
        );

        server.getNodeMap().addNode(writeOnlyFolder);
        rootNode.addOrganizes(writeOnlyFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, namespaceName + "/WriteOnly/" + name))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setUserAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("can't read this")));

        server.getNodeMap().addNode(node);
        writeOnlyFolder.addOrganizes(node);
    }

    private void addAdminReadableNodes(UaFolderNode rootNode) {
        UaFolderNode adminFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/OnlyAdminCanRead"),
            new QualifiedName(namespaceIndex, "OnlyAdminCanRead"),
            LocalizedText.english("OnlyAdminCanRead")
        );

        server.getNodeMap().addNode(adminFolder);
        rootNode.addOrganizes(adminFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, namespaceName + "/OnlyAdminCanRead/" + name))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("shh... don't tell the lusers")));

        node.setAttributeDelegate(new RestrictedAccessDelegate(identity -> {
            if ("admin".equals(identity)) {
                return AccessLevel.READ_WRITE;
            } else {
                return AccessLevel.NONE;
            }
        }));

        server.getNodeMap().addNode(node);
        adminFolder.addOrganizes(node);
    }

    private void addAdminWritableNodes(UaFolderNode rootNode) {
        UaFolderNode adminFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/OnlyAdminCanWrite"),
            new QualifiedName(namespaceIndex, "OnlyAdminCanWrite"),
            LocalizedText.english("OnlyAdminCanWrite")
        );

        server.getNodeMap().addNode(adminFolder);
        rootNode.addOrganizes(adminFolder);

        String name = "String";
        UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, namespaceName + "/OnlyAdminCanWrite/" + name))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, name))
            .setDisplayName(LocalizedText.english(name))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        node.setValue(new DataValue(new Variant("admin was here")));

        node.setAttributeDelegate(new RestrictedAccessDelegate(identity -> {
            if ("admin".equals(identity)) {
                return AccessLevel.READ_WRITE;
            } else {
                return AccessLevel.READ_ONLY;
            }
        }));

        server.getNodeMap().addNode(node);
        adminFolder.addOrganizes(node);
    }

    private void addDynamicNodes(UaFolderNode rootNode) {
        UaFolderNode dynamicFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/Dynamic"),
            new QualifiedName(namespaceIndex, "Dynamic"),
            LocalizedText.english("Dynamic")
        );

        server.getNodeMap().addNode(dynamicFolder);
        rootNode.addOrganizes(dynamicFolder);

        // Dynamic Boolean
        {
            String name = "Boolean";
            NodeId typeId = Identifiers.Boolean;
            Variant variant = new Variant(false);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
                .setNodeId(new NodeId(namespaceIndex, namespaceName + "/Dynamic/" + name))
                .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
                .setBrowseName(new QualifiedName(namespaceIndex, name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            AttributeDelegate delegate = AttributeDelegateChain.create(
                new AttributeDelegate() {
                    @Override
                    public DataValue getValue(AttributeContext context, VariableNode node) throws UaException {
                        return new DataValue(new Variant(random.nextBoolean()));
                    }
                },
                ValueLoggingDelegate::new
            );

            node.setAttributeDelegate(delegate);

            server.getNodeMap().addNode(node);
            dynamicFolder.addOrganizes(node);
        }

        // Dynamic Int32
        {
            String name = "Int32";
            NodeId typeId = Identifiers.Int32;
            Variant variant = new Variant(0);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
                .setNodeId(new NodeId(namespaceIndex, namespaceName + "/Dynamic/" + name))
                .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
                .setBrowseName(new QualifiedName(namespaceIndex, name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            AttributeDelegate delegate = AttributeDelegateChain.create(
                new AttributeDelegate() {
                    @Override
                    public DataValue getValue(AttributeContext context, VariableNode node) throws UaException {
                        return new DataValue(new Variant(random.nextInt()));
                    }
                },
                ValueLoggingDelegate::new
            );

            node.setAttributeDelegate(delegate);

            server.getNodeMap().addNode(node);
            dynamicFolder.addOrganizes(node);
        }

        // Dynamic Double
        {
            String name = "Double";
            NodeId typeId = Identifiers.Double;
            Variant variant = new Variant(0.0);

            UaVariableNode node = new UaVariableNode.UaVariableNodeBuilder(server.getNodeMap())
                .setNodeId(new NodeId(namespaceIndex, namespaceName + "/Dynamic/" + name))
                .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
                .setBrowseName(new QualifiedName(namespaceIndex, name))
                .setDisplayName(LocalizedText.english(name))
                .setDataType(typeId)
                .setTypeDefinition(Identifiers.BaseDataVariableType)
                .build();

            node.setValue(new DataValue(variant));

            AttributeDelegate delegate = AttributeDelegateChain.create(
                new AttributeDelegate() {
                    @Override
                    public DataValue getValue(AttributeContext context, VariableNode node) throws UaException {
                        return new DataValue(new Variant(random.nextDouble()));
                    }
                },
                ValueLoggingDelegate::new
            );

            node.setAttributeDelegate(delegate);

            server.getNodeMap().addNode(node);
            dynamicFolder.addOrganizes(node);
        }
    }

    private void addDataAccessNodes(UaFolderNode rootNode) {
        // DataAccess folder
        UaFolderNode dataAccessFolder = new UaFolderNode(
            server.getNodeMap(),
            new NodeId(namespaceIndex, namespaceName + "/DataAccess"),
            new QualifiedName(namespaceIndex, "DataAccess"),
            LocalizedText.english("DataAccess")
        );

        server.getNodeMap().addNode(dataAccessFolder);
        rootNode.addOrganizes(dataAccessFolder);

        // AnalogItemType node
        AnalogItemNode node = nodeFactory.createVariable(
            new NodeId(namespaceIndex, namespaceName + "/DataAccess/AnalogValue"),
            new QualifiedName(namespaceIndex, "AnalogValue"),
            LocalizedText.english("AnalogValue"),
            Identifiers.AnalogItemType,
            AnalogItemNode.class
        );

        node.setDataType(Identifiers.Double);
        node.setValue(new DataValue(new Variant(3.14d)));

        node.setEURange(new Range(0.0, 100.0));

        server.getNodeMap().addNode(node);
        dataAccessFolder.addOrganizes(node);
    }

    private void addMethodNode(UaFolderNode folderNode) {
        UaMethodNode methodNode = UaMethodNode.builder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, namespaceName + "/sqrt(x)"))
            .setBrowseName(new QualifiedName(namespaceIndex, "sqrt(x)"))
            .setDisplayName(new LocalizedText(null, "sqrt(x)"))
            .setDescription(
                LocalizedText.english("Returns the correctly rounded positive square root of a double value."))
            .build();


        try {
            AnnotationBasedInvocationHandler invocationHandler =
                AnnotationBasedInvocationHandler.fromAnnotatedObject(
                    server.getNodeMap(), new SqrtMethod());

            methodNode.setProperty(UaMethodNode.InputArguments, invocationHandler.getInputArguments());
            methodNode.setProperty(UaMethodNode.OutputArguments, invocationHandler.getOutputArguments());
            methodNode.setInvocationHandler(invocationHandler);

            server.getNodeMap().addNode(methodNode);

            folderNode.addReference(new Reference(
                folderNode.getNodeId(),
                Identifiers.HasComponent,
                methodNode.getNodeId().expanded(),
                methodNode.getNodeClass(),
                true
            ));

            methodNode.addReference(new Reference(
                methodNode.getNodeId(),
                Identifiers.HasComponent,
                folderNode.getNodeId().expanded(),
                folderNode.getNodeClass(),
                false
            ));
        } catch (Exception e) {
            logger.error("Error creating sqrt() method.", e);
        }
    }

    private void addCustomObjectTypeAndInstance(UaFolderNode rootFolder) throws UaException {
        // Define a new ObjectType called "MyObjectType".
        UaObjectTypeNode objectTypeNode = UaObjectTypeNode.builder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, "ObjectTypes/MyObjectType"))
            .setBrowseName(new QualifiedName(namespaceIndex, "MyObjectType"))
            .setDisplayName(LocalizedText.english("MyObjectType"))
            .setIsAbstract(false)
            .build();

        // "Foo" and "Bar" are members. These nodes are what are called "instance declarations" by the spec.
        UaVariableNode foo = UaVariableNode.builder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, "ObjectTypes/MyObjectType.Foo"))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, "Foo"))
            .setDisplayName(LocalizedText.english("Foo"))
            .setDataType(Identifiers.Int16)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        foo.setValue(new DataValue(new Variant(0)));
        objectTypeNode.addComponent(foo);

        UaVariableNode bar = UaVariableNode.builder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, "ObjectTypes/MyObjectType.Bar"))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, "Bar"))
            .setDisplayName(LocalizedText.english("Bar"))
            .setDataType(Identifiers.String)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        bar.setValue(new DataValue(new Variant("bar")));
        objectTypeNode.addComponent(bar);

        // Tell the ObjectTypeManager about our new type.
        // This let's us use NodeFactory to instantiate instances of the type.
        server.getObjectTypeManager().registerObjectType(
            objectTypeNode.getNodeId(),
            UaObjectNode.class,
            UaObjectNode::new
        );

        // Add our ObjectTypeNode as a subtype of BaseObjectType.
        server.getUaNamespace().addReference(
            Identifiers.BaseObjectType,
            Identifiers.HasSubtype,
            true,
            objectTypeNode.getNodeId().expanded(),
            NodeClass.ObjectType
        );

        // Add the inverse SubtypeOf relationship.
        objectTypeNode.addReference(new Reference(
            objectTypeNode.getNodeId(),
            Identifiers.HasSubtype,
            Identifiers.BaseObjectType.expanded(),
            NodeClass.ObjectType,
            false
        ));

        // Add it into the address space.
        server.getNodeMap().addNode(objectTypeNode);

        // Use NodeFactory to create instance of MyObjectType called "MyObject".
        // NodeFactory takes care of recursively instantiating MyObject member nodes
        // as well as adding all nodes to the address space.
        UaObjectNode myObject = nodeFactory.createObject(
            new NodeId(namespaceIndex, namespaceName + "/MyObject"),
            new QualifiedName(namespaceIndex, "MyObject"),
            LocalizedText.english("MyObject"),
            objectTypeNode.getNodeId()
        );

        // Add forward and inverse references from the root folder.
        rootFolder.addOrganizes(myObject);

        myObject.addReference(new Reference(
            myObject.getNodeId(),
            Identifiers.Organizes,
            rootFolder.getNodeId().expanded(),
            rootFolder.getNodeClass(),
            false
        ));
    }

    private void addCustomDataTypeVariable(UaFolderNode rootFolder) {
        // add a custom DataTypeNode as a subtype of the built-in Structure DataTypeNode
        NodeId dataTypeId = new NodeId(namespaceIndex, "DataType.CustomDataType");

        UaDataTypeNode dataTypeNode = new UaDataTypeNode(
            server.getNodeMap(),
            dataTypeId,
            new QualifiedName(namespaceIndex, "CustomDataType"),
            LocalizedText.english("CustomDataType"),
            LocalizedText.english("CustomDataType"),
            uint(0),
            uint(0),
            false
        );

        // Inverse ref to Structure
        dataTypeNode.addReference(new Reference(
            dataTypeId,
            Identifiers.HasSubtype,
            Identifiers.Structure.expanded(),
            NodeClass.DataType,
            false
        ));

        // Forward ref from Structure
        Optional<UaDataTypeNode> structureDataTypeNode = server.getNodeMap()
            .getNode(Identifiers.Structure)
            .map(UaDataTypeNode.class::cast);

        structureDataTypeNode.ifPresent(node ->
            node.addReference(new Reference(
                node.getNodeId(),
                Identifiers.HasSubtype,
                dataTypeId.expanded(),
                NodeClass.DataType,
                true
            ))
        );

        // Create a dictionary, binaryEncodingId, and register the codec under that id
        OpcUaBinaryDataTypeDictionary dictionary = new OpcUaBinaryDataTypeDictionary(
            "urn:siemens:opc:server:custom-data-type"
        );

        NodeId binaryEncodingId = new NodeId(namespaceIndex, "DataType.CustomDataType.BinaryEncoding");

        dictionary.registerStructCodec(
            new CustomDataType.Codec().asBinaryCodec(),
            "CustomDataType",
            binaryEncodingId
        );

        // Register dictionary with the shared DataTypeManager instance
        OpcUaDataTypeManager.getInstance().registerTypeDictionary(dictionary);


        UaVariableNode customDataTypeVariable = UaVariableNode.builder(server.getNodeMap())
            .setNodeId(new NodeId(namespaceIndex, namespaceName + "/CustomDataTypeVariable"))
            .setAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setUserAccessLevel(ubyte(AccessLevel.getMask(AccessLevel.READ_WRITE)))
            .setBrowseName(new QualifiedName(namespaceIndex, "CustomDataTypeVariable"))
            .setDisplayName(LocalizedText.english("CustomDataTypeVariable"))
            .setDataType(dataTypeId)
            .setTypeDefinition(Identifiers.BaseDataVariableType)
            .build();

        CustomDataType value = new CustomDataType(
            "foo",
            uint(42),
            true
        );

        ExtensionObject xo = ExtensionObject.encode(value, binaryEncodingId);

        customDataTypeVariable.setValue(new DataValue(new Variant(xo)));

        rootFolder.addOrganizes(customDataTypeVariable);

        customDataTypeVariable.addReference(new Reference(
            customDataTypeVariable.getNodeId(),
            Identifiers.Organizes,
            rootFolder.getNodeId().expanded(),
            rootFolder.getNodeClass(),
            false
        ));
    }

    @Override
    public CompletableFuture<List<Reference>> browse(AccessContext context, NodeId nodeId) {
        ServerNode node = server.getNodeMap().get(nodeId);

        if (node != null) {
            return CompletableFuture.completedFuture(node.getReferences());
        } else {
            return FutureUtils.failedFuture(new UaException(StatusCodes.Bad_NodeIdUnknown));
        }
    }

    @Override
    public void read(
        ReadContext context,
        Double maxAge,
        TimestampsToReturn timestamps,
        List<ReadValueId> readValueIds) {

        List<DataValue> results = Lists.newArrayListWithCapacity(readValueIds.size());

        for (ReadValueId readValueId : readValueIds) {
            ServerNode node = server.getNodeMap().get(readValueId.getNodeId());

            if (node != null) {
                DataValue value = node.readAttribute(
                    new AttributeContext(context),
                    readValueId.getAttributeId(),
                    timestamps,
                    readValueId.getIndexRange(),
                    readValueId.getDataEncoding()
                );

                results.add(value);
            } else {
                results.add(new DataValue(StatusCodes.Bad_NodeIdUnknown));
            }
        }

        context.complete(results);
    }

    @Override
    public void write(WriteContext context, List<WriteValue> writeValues) {
        List<StatusCode> results = Lists.newArrayListWithCapacity(writeValues.size());

        for (WriteValue writeValue : writeValues) {
            ServerNode node = server.getNodeMap().get(writeValue.getNodeId());

            if (node != null) {
                try {
                    node.writeAttribute(
                        new AttributeContext(context),
                        writeValue.getAttributeId(),
                        writeValue.getValue(),
                        writeValue.getIndexRange()
                    );

                    results.add(StatusCode.GOOD);

                    logger.info(
                        "Wrote value {} to {} attribute of {}",
                        writeValue.getValue().getValue(),
                        AttributeId.from(writeValue.getAttributeId()).map(Object::toString).orElse("unknown"),
                        node.getNodeId());
                } catch (UaException e) {
                    logger.error("Unable to write value={}", writeValue.getValue(), e);
                    results.add(e.getStatusCode());
                }
            } else {
                results.add(new StatusCode(StatusCodes.Bad_NodeIdUnknown));
            }
        }

        context.complete(results);
    }

    @Override
    public void onDataItemsCreated(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsCreated(dataItems);
    }

    @Override
    public void onDataItemsModified(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsModified(dataItems);
    }

    @Override
    public void onDataItemsDeleted(List<DataItem> dataItems) {
        subscriptionModel.onDataItemsDeleted(dataItems);
    }

    @Override
    public void onMonitoringModeChanged(List<MonitoredItem> monitoredItems) {
        subscriptionModel.onMonitoringModeChanged(monitoredItems);
    }

    @Override
    public Optional<MethodInvocationHandler> getInvocationHandler(NodeId methodId) {
        Optional<ServerNode> node = server.getNodeMap().getNode(methodId);

        return node.flatMap(n -> {
            if (n instanceof UaMethodNode) {
                return ((UaMethodNode) n).getInvocationHandler();
            } else {
                return Optional.empty();
            }
        });
    }

}