import { createHubEnvironmentAtoms } from "@t3tools/client-runtime/state/hub";

import { connectionAtomRuntime } from "../connection/runtime";

export const hubEnvironment = createHubEnvironmentAtoms(connectionAtomRuntime);
