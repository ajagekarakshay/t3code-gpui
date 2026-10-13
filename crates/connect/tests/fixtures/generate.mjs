// Synthetic interoperability fixtures, generated with Node's independent JOSE primitives.
import { generateKeyPairSync, sign, createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
const encode = (value) => Buffer.from(typeof value === "string" ? value : JSON.stringify(value)).toString("base64url");
const jwt = (header, claims, key, algorithm = null) => {
  const body = `${encode(header)}.${encode(claims)}`;
  return `${body}.${sign(algorithm, Buffer.from(body), { key, dsaEncoding: "ieee-p1363" }).toString("base64url")}`;
};
const ed = generateKeyPairSync("ed25519");
const ec = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
const jwk = ec.publicKey.export({ format: "jwk" });
const thumbprint = createHash("sha256").update(JSON.stringify({ crv: jwk.crv, kty: jwk.kty, x: jwk.x, y: jwk.y })).digest("base64url");
const relayClaims = { iss: "https://relay.fixture.test", aud: "t3-env:fixture", sub: "cloud-user", jti: "relay-jti", iat: 1800000000, exp: 1800000060, nonce: "relay-nonce", environmentId: "fixture", clientProofKeyThumbprint: thumbprint, cnf: { jkt: thumbprint }, scope: ["environment:connect"] };
const dpopClaims = { htm: "GET", htu: "https://environment.fixture.test/api/auth/session", iat: 1800000000, jti: "client-jti", ath: createHash("sha256").update("fixture-access-token").digest("base64url") };
const fixture = {
  relayPublicKey: ed.publicKey.export({ type: "spki", format: "pem" }),
  // Test-only key; never used by an environment or relay.
  relayPrivateKey: ed.privateKey.export({ type: "pkcs8", format: "pem" }),
  relayClaims,
  relayProof: jwt({ alg: "EdDSA", typ: "t3-cloud-mint+jwt" }, relayClaims, ed.privateKey),
  dpopProof: jwt({ alg: "ES256", typ: "dpop+jwt", jwk }, dpopClaims, ec.privateKey, "sha256"),
  thumbprint,
};
writeFileSync(new URL("connect.json", import.meta.url), `${JSON.stringify(fixture, null, 2)}\n`);
