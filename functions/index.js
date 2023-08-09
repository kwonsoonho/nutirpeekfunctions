/**
 * Import function triggers from their respective submodules:
 *
 * const {onCall} = require("firebase-functions/v2/https");
 * const {onDocumentWritten} = require("firebase-functions/v2/firestore");
 *
 * See a full list of supported triggers at https://firebase.google.com/docs/functions
 */

// const {onRequest} = require("firebase-functions/v2/https");
// const logger = require("firebase-functions/logger");

// Create and deploy your first functions
// https://firebase.google.com/docs/functions/get-started

// exports.helloWorld = onRequest((request, response) => {
//   logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });


const fetch = require("node-fetch");
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const serviceAccount = require("./serviceAccountKey.json");

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});

const db = admin.firestore();

const errorMessages = {
  "INFO-000": "정상 처리되었습니다.",
  "ERROR-300": "필수 값이 누락되어 있습니다. 요청인자를 참고 하십시오.",
  "INFO-100": "인증키가 유효하지 않습니다. 인증키가 없는 경우, 홈페이지에서 인증키를 신청하십시오.",
  "ERROR-301": "파일타입 값이 누락 혹은 유효하지 않습니다. 요청인자 중 TYPE을 확인하십시오.",
  "ERROR-310": "해당하는 서비스를 찾을 수 없습니다. 요청인자 중 SERVICE를 확인하십시오.",
  "ERROR-331": "요청시작위치 값을 확인하십시오. 요청인자 중 START_INDEX를 확인하십시오.",
  "ERROR-332": "요청종료위치 값을 확인하십시오. 요청이자 중 END_INDEX값을 확인하십시오.",
  "ERROR-334": "종료위치보다 시작위치가 더 큽니다. 요청시작조회건수는 정수를 입력하세요.",
  "ERROR-336": "데이터요청은 한번에 최대 1000건을 넘을 수 없습니다.",
  "ERROR-500": "서버오류입니다.",
  "ERROR-601": "SQL 문장 오류입니다.",
  "INFO-200": "해당하는 데이터가 없습니다.",
  "INFO-300": "유효 호출건수를 이미 초과하셨습니다.",
  "INFO-400": "권한이 없습니다. 관리자에게 문의하십시오.",
};

/**
 * 보충제 데이터를 가져오는 함수입니다.
 * @param {number} startIdx 시작 인덱스
 * @param {number} limit 한번에 가져올 데이터 수
 * @return {Promise<Object>} 응답 데이터 객체
 */
async function fetchSupplementsData(startIdx, limit) {
  const apiDefaultUrl = "http://openapi.foodsafetykorea.go.kr/api/";
  const apiCode = "I0030";
  const apiKey = "a4282c0b1ecc4aeb9391";
  const maxRetry = 3; // 최대 재시도 횟수

  let retries = 0;
  let retryDelay = 2000; // 시작 대기 시간은 2초
  while (retries < maxRetry) {
    try {
      const response = await fetch(`${apiDefaultUrl}${apiKey}/${apiCode}/json/${startIdx}/${startIdx + limit - 1}`);
      const data = await response.json();

      // 결과 코드 확인
      const resultCode = data["I0030"].RESULT.CODE;
      if (errorMessages[resultCode] && resultCode !== "INFO-000") {
        console.error(`API 에러 (${resultCode}): `, errorMessages[resultCode]);
        throw new Error(errorMessages[resultCode]);
      }

      return data;
    } catch (error) {
      retries++;
      if (retries === maxRetry) {
        throw new Error("API 요청에 실패하였습니다.");
      }
      await new Promise((resolve) => setTimeout(resolve, retryDelay));
      retryDelay *= 2; // 대기 시간 두배로 증가
    }
  }
}

/**
 * 가져온 데이터를 Firestore에 저장하는 함수입니다.
 * @param {Object} data API로부터 가져온 데이터 객체
 */

async function storeSupplementsInFirestore(data) {
  const apiCode = "I0030";
  const items = data[apiCode].row;
  const batchCommits = [];
  const chunkSize = 100;

  for (let i = 0; i < items.length; i += chunkSize) {
    const batch = db.batch();
    const chunk = items.slice(i, i + chunkSize);

    for (const supplementData of chunk) {
      const documentRef = db.collection("supplements").doc(supplementData.PRDLST_REPORT_NO);
      const supplementInfo = {
        lcnsNo: supplementData.LCNS_NO,
        businessName: supplementData.BSSH_NM,
        productReportNo: supplementData.PRDLST_REPORT_NO,
        productName: supplementData.PRDLST_NM,
        permissionDate: supplementData.PRMS_DT,
        expirationDays: supplementData.POG_DAYCNT,
        shape: supplementData.DISPOS,
        intakeMethod: supplementData.NTK_MTHD,
        primaryFunctionality: supplementData.PRIMARY_FNCLTY,
        cautionWhenTaking: supplementData.IFTKN_ATNT_MATR_CN,
        storageMethod: supplementData.CSTDY_MTHD,
        type: supplementData.PRDLST_CDNM,
        standardSpecification: supplementData.STDR_STND,
        highCalorieLowNutrition: supplementData.HIENG_LNTRT_DVS_NM,
        productionEnd: supplementData.PRODUCTION,
        childFriendlyCertification: supplementData.CHILD_CRTFC_YN,
        productForm: supplementData.PRDT_SHAP_CD_NM,
        packagingMaterial: supplementData.FRMLC_MTRQLT,
        productType: supplementData.RAWMTRL_NM,
        industryType: supplementData.INDUTY_CD_NM,
        lastUpdateDate: supplementData.LAST_UPDT_DTM,
        functionalIngredients: supplementData.INDIV_RAWMTRL_NM,
        otherIngredients: supplementData.ETC_RAWMTRL_NM,
        capsuleIngredients: supplementData.CAP_RAWMTRL_NM,
      };


      batch.set(documentRef, supplementInfo, {merge: true});
    }

    batchCommits.push(batch.commit());
  }
  await Promise.all(batchCommits);
}

/**
 * @param {number} startIdx 시작 인덱스
 * 보충제 데이터를 가져오고 Firestore에 저장하는 함수입니다.
 */
async function fetchAndStoreSupplements(startIdx) {
  let continueFetching = true;
  const limit = 1000; // 한 번에 가져올 데이터 수
  let totalFetchedCount = 0; // 총 가져온 데이터 수

  while (continueFetching) {
    const data = await fetchSupplementsData(startIdx, limit);
    const resultCode = data["I0030"].RESULT.CODE;

    if (!errorMessages[resultCode]) {
      throw new Error("알 수 없는 에러 코드: " + resultCode);
    }

    // ERROR-332 코드 또는 데이터가 없는 경우 반복문 종료
    if (resultCode === "INFO-200" || !data["I0030"].row || data["I0030"].row.length === 0) {
      continueFetching = false;
      continue;
    }

    if (resultCode !== "INFO-000") {
      return data["I0030"].RESULT.MSG;
    }

    const fetchedCount = data["I0030"].row.length;
    totalFetchedCount += fetchedCount;

    await storeSupplementsInFirestore(data);

    if (fetchedCount < limit) {
      continueFetching = false; // 가져온 데이터의 수가 limit 미만이면 더 이상 데이터가 없다고 판단하고 반복문 종료
      continue;
    }

    startIdx += limit; // 다음 데이터를 가져오기 위해 시작 인덱스 업데이트
  }

  return `총 조회된 데이터 수: ${totalFetchedCount}`;
}
async function convertOtherIngredientsToStringArray() {
  const supplementsCollection = db.collection("supplements");
  const snapshot = await supplementsCollection.get();

  const batchCommits = [];
  const chunkSize = 500;
  const allDocuments = [];

  snapshot.forEach((doc) => {
    allDocuments.push(doc);
  });

  for (let i = 0; i < allDocuments.length; i += chunkSize) {
    const batch = db.batch();
    const chunk = allDocuments.slice(i, i + chunkSize);

    for (const doc of chunk) {
      const data = doc.data();
      if (typeof data.otherIngredients === "string") {
        const ingredientsArray = data.otherIngredients.split(",").map((ingredient) => ingredient.trim());
        batch.update(doc.ref, {otherIngredients: ingredientsArray});
      }
    }

    batchCommits.push(batch.commit());
  }

  await Promise.all(batchCommits);
  return `변환된 문서 수: ${allDocuments.length}`;
}

exports.convertIngredients = functions
    .runWith({timeoutSeconds: 540})
    .https.onRequest(async (req, res) => {
      try {
        const result = await convertOtherIngredientsToStringArray();
        res.status(200).send(result);
      } catch (error) {
        console.error("보충제 데이터 변환 중 오류 발생:", error);
        res.status(500).send(error.message);
      }
    });

exports.runSupplementImport = functions
    .runWith({timeoutSeconds: 300})
    .https.onRequest(async (req, res) => {
      try {
        const startIdx = Number(req.query.startIdx) || 0; // startIdx 값을 쿼리 문자열에서 가져옴, 없으면 0으로 시작
        const result = await fetchAndStoreSupplements(startIdx);
        res.status(200).send(result);
      } catch (error) {
        console.error("보충제 데이터 가져오기 및 저장 중 오류 발생:", error);
        res.status(500).send(error.message);
      }
    });
