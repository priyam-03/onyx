import { PopupSpec } from "@/components/admin/connectors/Popup";
import { createConnector, runConnector } from "@/lib/connector";
import { createCredential, linkCredential } from "@/lib/credential";
import { FolderConfig } from "@/lib/connectors/connectors";
import { AccessType, ValidSources } from "@/lib/types";

export const submitFolder = async (
  folderPath: string,
  setPopup: (popup: PopupSpec) => void,
  name: string,
  access_type: string,
  groups?: number[]
) => {
  if (!folderPath || folderPath.trim() === "") {
    setPopup({
      message: "Folder path cannot be empty",
      type: "error",
    });
    return false;
  }

  const [connectorErrorMsg, connector] = await createConnector<FolderConfig>({
    name: "FolderConnector-" + Date.now(),
    source: ValidSources.Folder,
    input_type: "load_state",
    connector_specific_config: {
      folder_path: folderPath.trim(),
    },
    refresh_freq: null,
    prune_freq: null,
    indexing_start: null,
    access_type: access_type,
    groups: groups,
  });
  if (connectorErrorMsg || !connector) {
    setPopup({
      message: `Unable to create connector - ${connectorErrorMsg}`,
      type: "error",
    });
    return false;
  }

  // Since there is no "real" credential associated with a folder connector
  // we create a dummy one here so that we can associate the CC Pair with a
  // user. This is needed since the user for a CC Pair is found via the credential
  // associated with it.
  const createCredentialResponse = await createCredential({
    credential_json: {},
    admin_public: true,
    source: ValidSources.Folder,
    curator_public: true,
    groups: groups,
    name,
  });
  if (!createCredentialResponse.ok) {
    const errorMsg = await createCredentialResponse.text();
    setPopup({
      message: `Error creating credential for CC Pair - ${errorMsg}`,
      type: "error",
    });
    return false;
  }
  const credentialId = (await createCredentialResponse.json()).id;

  const credentialResponse = await linkCredential(
    connector.id,
    credentialId,
    name,
    access_type as AccessType,
    groups
  );
  if (!credentialResponse.ok) {
    const credentialResponseJson = await credentialResponse.json();
    setPopup({
      message: `Unable to link connector to credential - ${credentialResponseJson.detail}`,
      type: "error",
    });
    return false;
  }

  const runConnectorErrorMsg = await runConnector(connector.id, [0]);
  if (runConnectorErrorMsg) {
    setPopup({
      message: `Unable to run connector - ${runConnectorErrorMsg}`,
      type: "error",
    });
    return false;
  }

  setPopup({
    type: "success",
    message: "Successfully created folder connector!",
  });
  return true;
};

