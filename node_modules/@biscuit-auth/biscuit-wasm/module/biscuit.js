import * as wasm from "./biscuit_bg.wasm";
import { __wbg_set_wasm } from "./biscuit_bg.js";
__wbg_set_wasm(wasm);
export * from "./biscuit_bg.js";

wasm.__wbindgen_start();
export function middleware(options) {
  // assumes the token is in the `Authorization` header,
  // prefixed with `Bearer `
  const defaultExtractor = function (req) {
    const authHeader = req.headers.authorization;
    if (!authHeader) {
      throw new Error("Missing Authorization header");
    }
    if (!authHeader.startsWith("Bearer ")) {
      throw new Error("Authorization header does not carry a bearer token");
    }

    return authHeader.slice(7);
  };

  const defaultParser = function (data, publicKey) {
    return Biscuit.fromBase64(data, publicKey);
  };

  const defaultOnError = function (errorType, error, req, res, next) {
    if (error instanceof Error) {
      console.error(`Failed ${errorType}: ${error.toString()}`);
    } else {
      console.error(`Failed ${errorType}: ${JSON.stringify(error)}`);
    }

    switch (errorType) {
      case "extraction":
        res.status(401).send();
        return;
      case "verification":
        res.status(403).send();
        return;
      case "authorization":
        res.status(403).send();
        return;
      default:
        return;
    }
  };

  const applyAuthorizerBuilder = (authorizer, makeAuthorizer, req) => {
    if (typeof makeAuthorizer === "function") {
      authorizer.merge(makeAuthorizer(req));
    } else if (makeAuthorizer) {
      authorizer.merge(makeAuthorizer);
    }
  };

  const { publicKey, priorityAuthorizer, fallbackAuthorizer } = options;
  const tokenExtractor = options.tokenExtractor ?? defaultExtractor;
  const tokenParser = options.tokenParser ?? defaultParser;
  const onError = options.onError ?? defaultOnError;

  return function (makeAuthorizer) {
    return function (req, res, next) {
      try {
        const serializedToken = tokenExtractor(req);
        try {
          const token = tokenParser(serializedToken, publicKey);
          try {
            let authorizer = new Authorizer();
            applyAuthorizerBuilder(authorizer, priorityAuthorizer, req);
            applyAuthorizerBuilder(authorizer, makeAuthorizer, req);
            applyAuthorizerBuilder(authorizer, fallbackAuthorizer, req);

            authorizer.addToken(token);
            const result = authorizer.authorize();
            req.biscuit = {
              token,
              authorizer,
              result,
            };
            next();
          } catch (e) {
            onError("authorization", e, req, res, next);
          }
        } catch (e) {
          onError("verification", e, req, res, next);
        }
      } catch (e) {
        onError("extraction", e, req, res, next);
      }
    };
  };
}
import {
  Biscuit,
  Authorizer,
  Rule,
  Fact,
  Check,
  Policy,
} from "./biscuit_bg.js";

export function bytesToHex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function prepareTerm(value) {
  if (value instanceof Date) {
    return { date: value.toISOString() };
  } else if (value instanceof Uint8Array) {
    return { bytes: bytesToHex(value) };
  } else if (Array.isArray(value)) {
    return value.map(prepareTerm);
  } else if (typeof value.toDatalogParameter === "function") {
    return value.toDatalogParameter();
  } else {
    return value;
  }
}

function tagged(builder) {
  return (strings, ...values) => {
    let code = "";
    for (let i = 0; i < strings.length; i++) {
      code += strings[i];
      if (i < values.length) {
        code += `{_param_${i}}`;
      }
    }

    const termParameters = Object.fromEntries(
      values.map((v, i) => {
        return [`_param_${i}`, prepareTerm(v)];
      })
    );

    const isKeyParam = (v) => {
      return (
        (typeof v === "string" && v.startsWith("ed25519/")) ||
        v.toDatalogParameter
      );
    };

    const keyParameters = Object.fromEntries(
      values
        .map((v, i) => [i, v])
        .filter(([i, v]) => isKeyParam(v))
        .map(([i, v]) => {
          return [`_param_${i}`, prepareTerm(v)];
        })
    );

    builder.addCodeWithParameters(code, termParameters, keyParameters);
    return builder;
  };
}

export function biscuit(strings, ...values) {
  const builder = Biscuit.builder();
  return tagged(builder)(strings, ...values);
}

export function block(strings, ...values) {
  const builder = Biscuit.block_builder();
  return tagged(builder)(strings, ...values);
}

export function authorizer(strings, ...values) {
  const builder = new Authorizer();
  return tagged(builder)(strings, ...values);
}

export function fact(strings, ...values) {
  let code = "";
  for (let i = 0; i < strings.length; i++) {
    code += strings[i];
    if (i < values.length) {
      code += `{_param_${i}}`;
    }
  }

  const params = new Map(
    values.map((v, i) => {
      return [`_param_${i}`, prepareTerm(v)];
    })
  );

  const f = Fact.fromString(code);
  const unboundParams = f.unboundParameters();

  for (let p of unboundParams) {
    f.set(p, params.get(p));
  }

  return f;
}

export function rule(strings, ...values) {
  let code = "";
  for (let i = 0; i < strings.length; i++) {
    code += strings[i];
    if (i < values.length) {
      code += `{_param_${i}}`;
    }
  }

  const params = new Map(
    values.map((v, i) => {
      return [`_param_${i}`, prepareTerm(v)];
    })
  );

  const r = Rule.fromString(code);
  const unboundParams = r.unboundParameters();
  const unboundScopeParams = r.unboundScopeParameters();

  for (let p of unboundParams) {
    r.set(p, params.get(p));
  }

  for (let p of unboundScopeParams) {
    r.setScope(p, params.get(p));
  }

  return r;
}

export function check(strings, ...values) {
  let code = "";
  for (let i = 0; i < strings.length; i++) {
    code += strings[i];
    if (i < values.length) {
      code += `{_param_${i}}`;
    }
  }

  const params = new Map(
    values.map((v, i) => {
      return [`_param_${i}`, prepareTerm(v)];
    })
  );

  const c = Check.fromString(code);
  const unboundParams = c.unboundParameters();
  const unboundScopeParams = c.unboundScopeParameters();

  for (let p of unboundParams) {
    c.set(p, params.get(p));
  }

  for (let p of unboundScopeParams) {
    c.setScope(p, params.get(p));
  }

  return c;
}

export function policy(strings, ...values) {
  let code = "";
  for (let i = 0; i < strings.length; i++) {
    code += strings[i];
    if (i < values.length) {
      code += `{_param_${i}}`;
    }
  }

  const params = new Map(
    values.map((v, i) => {
      return [`_param_${i}`, prepareTerm(v)];
    })
  );

  const pol = Policy.fromString(code);
  const unboundParams = pol.unboundParameters();
  const unboundScopeParams = pol.unboundScopeParameters();

  for (let p of unboundParams) {
    pol.set(p, params.get(p));
  }

  for (let p of unboundScopeParams) {
    pol.setScope(p, params.get(p));
  }

  return pol;
}
